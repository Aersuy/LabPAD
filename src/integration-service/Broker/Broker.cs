using db_service.Interfaces;
using shared;
using shared.Enums;
using shared.IImplementations;
using shared.Interfaces;
using shared.Logging;
using shared.Models;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;

namespace Broker
{
    public class Broker
    {
        private readonly IPAddress _ip;
        private readonly int _port;
        private Socket? _listenerSocket;
        private IMessageService2 _messageService;
        private readonly FileLogger _logger;
        private readonly ConcurrentDictionary<Guid, ReceiverConnection> _receivers = new();

        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
        };


        public Broker(IPAddress ip, int port, IMessageService2 messageService,FileLogger logger)
        {
            _ip = ip;
            _port = port;
            _messageService = messageService;
            _logger = logger;
        }
        public void Start()
        {
            _listenerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _listenerSocket.Bind(new IPEndPoint(_ip, _port));
            _listenerSocket.Listen();

            _logger.Log("Broker", "Info", "Broker listening");
        }
        public async Task RunAsync()
        {

            while (true)
            {
                Socket clientSocket = await _listenerSocket!.AcceptAsync();
                _ = HandleClientAsync(clientSocket);
            }
        }
        private async Task HandleClientAsync(Socket socket)
        {
            ITransport transport = new VladTransport(socket);
            try
            {
                MessageEnvelope? registerMsg;
                try
                {
                    registerMsg = await MessageProtocol.ReadMessageAsync(transport);

                }
                catch (Exception ex) when (ex is EndOfStreamException or InvalidDataException)
                {
                    _logger.Log("Broker", "Warning", "Registration failed");
                    transport.close();
                    return;
                }
                if (registerMsg is null || registerMsg.MessageType != MessageType.Register)
                {
                    transport.close();
                    return;
                }
                RegisterPayload? payload;
                try
                {
                    payload = registerMsg.JsonPayload.Deserialize<RegisterPayload>(JsonOptions)
                        ?? throw new JsonException("Empty register payload");
                }
                catch (Exception ex) when (ex is JsonException or InvalidOperationException or NotSupportedException)
                {
                    await SendErrorMessageAsync(transport, "InvalidRegisterPayload", ex.Message);
                    transport.close();
                    return;
                }
                await SendAckAsync(transport, registerMsg.SenderId, registerMsg.MessageId);

                switch (payload.Role)
                {
                    case Roles.Subscriber:
                        await HandleReceiverAsync(registerMsg.SenderId, transport, registerMsg.Subject);
                        break;
                    case Roles.Sender:
                        await HandleSenderAsync(registerMsg.SenderId, transport);
                        break;
                    default:
                        _logger.Log("Broker", "Warning", "Unrecognised role");
                        transport.close();
                        break;
                }
            }
            catch (Exception ex)
            {
                _logger.Log("Broker", "Warning", $"HandleClientAsync threw: {ex.GetType().Name}: {ex.Message}");
            }
            finally
            {
                transport.close();
            }
        }
        private static async Task SendErrorMessageAsync(ITransport transport, string message, string aditionalData = "")
        {
            List<string> subject = new List<string>();
            subject.Add(message);
            var payload = JsonSerializer.SerializeToElement(new
            {
                AdditionalData = aditionalData
            });
            var msg = new MessageEnvelope
            {
                MessageType = MessageType.Error,
                MessageId = Guid.CreateVersion7(),
                TimeStamp = DateTime.UtcNow,
                Subject = subject,
                JsonPayload = payload
            };
            await MessageProtocol.WriteMessageAsync(transport, msg);
        }
        private static async Task SendAckAsync(ITransport transport, Guid targetId, Guid messageAcknowledged)
        {
            var payload = JsonSerializer.SerializeToElement(new AckPayload { MessageAcknowledged = messageAcknowledged }, JsonOptions);
            var ack = new MessageEnvelope
            {
                MessageType = MessageType.Ack,
                MessageId = Guid.NewGuid(),
                SenderId = targetId,
                TimeStamp = DateTime.UtcNow,
                JsonPayload = payload
            };
            await MessageProtocol.WriteMessageAsync(transport, ack);
        }
        private async Task HandleReceiverAsync(Guid id, ITransport transport, List<string> subject)
        {
            var connection = new ReceiverConnection(id, transport, subject);

            ReceiverConnection? previous = null;
            _receivers.AddOrUpdate(id, connection, (_, existing) => { previous = existing; return connection; });

            using var sentCts = new CancellationTokenSource();
            var sendLoop = ReceiverSendLoopAsync(connection, sentCts.Token);
            previous?.Transport.close();
            _logger.Log("Broker", "Info", $"Receiver {id} registered for subjects : {string.Join(", ", subject)}");
            try
            {
                while (true)
                {
                    MessageEnvelope? msg = await MessageProtocol.ReadMessageAsync(transport);
                    if (msg is null)
                    {
                        break;
                    }
                    await HandleReceiverMessageAsync(id, msg);
                }
            }
            catch (Exception ex) when (ex is EndOfStreamException or InvalidDataException
                                  or IOException or SocketException or ObjectDisposedException)
            {
                _logger.Log("Broker", "Warning", $"Receiver {id} connection error: {ex.Message}");
            }
            finally
            {
                // Old version, remove the receiver from the dictionary and close the transport
                // Just removes th item with the id, connection doesn't matter
                //_receivers.TryRemove(id, out _);


                // Remove the reicever only if the connection matches, to avoid removing a new connection with the same id
                // Can't just check if the connection is the same because of atomicity, so we check and remove in one operation
                sentCts.Cancel();
                _receivers.TryRemove(new KeyValuePair<Guid, ReceiverConnection>(id, connection));
                transport.close();
                await sendLoop;
                _logger.Log("Broker", "Info", $"Receiver {id} disconnected.");
            }
        }
        private const int MaxAttempts = 5;
        private static readonly TimeSpan AckTimeout = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);

        private async Task ReceiverSendLoopAsync(ReceiverConnection receiver, CancellationToken ct)
        {
            var receiverIds = new[] { receiver.Id };
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var deliveries = await _messageService.GetDueDeliveriesAsync(DateTime.UtcNow, receiverIds, 10);
                        foreach (var delivery in deliveries)
                        {
                            ct.ThrowIfCancellationRequested();
                            if (delivery.Attempts >= MaxAttempts)
                            {
                                await _messageService.MarkDeadLetterAsync(delivery.MessageId, delivery.ReceiverId, "Max attempts");
                                continue;
                            }
                            var envelope = await _messageService.LoadEnvelopeAsync(delivery.MessageId);
                            if (envelope is null)
                                continue;
                            var now = DateTime.UtcNow;
                            int updated = await _messageService.RecordAttemptAsync(
                                delivery.MessageId, delivery.ReceiverId, now, now + AckTimeout);
                            if (updated != 1)
                            {
                                continue;
                            }
                            using var sendTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                            sendTimeoutCts.CancelAfter(SendTimeout);
                            try
                            {
                                await MessageProtocol.WriteMessageAsync(receiver.Transport, envelope, sendTimeoutCts.Token);
                                _logger.Log("Broker", "Info", $"Sent {envelope.MessageId} to {delivery.ReceiverId} (attempt {delivery.Attempts + 1})");
                            }
                            //Catch timeout exception
                            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                            {
                                _logger.Log("Broker", "Warning", $"Send to {delivery.ReceiverId} timed out");
                                receiver.Transport.close();
                                return;
                            }
                            //Catch other exceptions
                            catch (Exception ex) when (!ct.IsCancellationRequested)
                            {
                                _logger.Log("Broker","Warning", $"Send to {delivery.ReceiverId} failed: {ex.Message}");
                                receiver.Transport.close();
                                return;
                            }
                        }
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception ex)
                    {
                        // Database error
                        _logger.Log("Broker", "Error", $"Dispatcher error for {receiver.Id}: {ex.GetType().Name}: {ex.Message}");
                    }

                    await Task.Delay(PollInterval, ct);
                }
            }
            catch (OperationCanceledException)
            {
                // Normal
            }
        }

        private async Task HandleReceiverMessageAsync(Guid receiverId, MessageEnvelope msg)
        {
            try
            {
                switch (msg.MessageType)
                {
                    case MessageType.Ack:
                        var ack = msg.JsonPayload.Deserialize<AckPayload>(JsonOptions);
                        if (ack is null)
                        {
                            break;
                        }
                        _logger.Log("Broker","Info", $"Receiver {receiverId} ACKed {ack.MessageAcknowledged}");
                        await _messageService.MarkAckedAsync(ack.MessageAcknowledged, receiverId);
                        break;
                    case MessageType.Nack:
                        var nack = msg.JsonPayload.Deserialize<NackPayload>(JsonOptions);
                        if (nack is null)
                        {
                            break;
                        }
                        _logger.Log("Broker","Warning", $"Receiver {receiverId} NACKed {nack.MessageNacked} with reason: {nack.Reason}, retryable: {nack.Retryable}");
                        if (nack.Retryable)
                        {
                            await _messageService.MarkRetryLaterAsync(nack.MessageNacked, receiverId, nack.Reason, DateTime.UtcNow + TimeSpan.FromSeconds(5));
                        }
                        else
                        {
                            await _messageService.MarkDeadLetterAsync(nack.MessageNacked, receiverId, nack.Reason);
                        }
                        break;
                    default:
                        _logger.Log("Broker", "Warning", $"Receiver {receiverId} sent unrecognized message type: {msg.MessageType}");
                        break;
                }
            }
            catch (JsonException ex)
            {
                _logger.Log("Broker", "Warning", $"Failed to deserialize message from receiver {receiverId}: {ex.Message}");
            }
            catch (Exception ex)
            {
                _logger.Log("Broker", "Error", $"Failed to record {msg.MessageType} from receiver {receiverId}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        // Handles the sender connection, checks for new messages,
        // stores them if they are new, sends an acknowledgment back to the sender even if
        // the message is a duplicate, dispatches to all receivers that are subscribed
        private async Task HandleSenderAsync(Guid id, ITransport transport)
        {
            _logger.Log("Broker", "Info", $"Sender {id} registered");
            try
            {
                while (true)
                {
                    MessageEnvelope? msg = await MessageProtocol.ReadMessageAsync(transport);
                    if (msg is null)
                    {
                        break;
                    }
                    _logger.Log("Broker", "Info", $"Sender {id} sent {msg.MessageType} for subjects: {string.Join(", ", msg.Subject)}");
                    if (msg.MessageType == MessageType.Data)
                    {
                        var targetIds = GetReceiverIdsForSubjects(msg);

                        bool isNew;
                        try
                        {
                            isNew = await _messageService.StoreMessageIfNewAsync(msg, targetIds);
                        }
                        catch (Exception ex)
                        {
                            _logger.Log("Broker", "Error", $"Store failed for {msg.MessageId}: {ex.Message}");

                            continue;     // Skip sending ack if store fails
                        }

                        _logger.Log("Broker", "Info", $"Message {msg.MessageId}: {(isNew ? "stored" : "duplicate")}, {targetIds.Count} target receiver(s)");
                        await SendAckAsync(transport, id, msg.MessageId);
                    }
                }

            }
            catch (Exception ex) when (ex is EndOfStreamException or InvalidDataException)
            {
                _logger.Log("Broker", "Warning", $"Sender {id} connection error");
            }
            finally
            {
                transport.close();
                _logger.Log("Broker", "Info", $"Sender {id} disconnected");
            }
        }
        private IReadOnlyCollection<Guid> GetReceiverIdsForSubjects(MessageEnvelope msg) =>
         _receivers.Values
        .Where(r => r.Subject.Intersect(msg.Subject).Any())
        .Select(r => r.Id)
        .ToList();
    }
    internal sealed class ReceiverConnection
    {
        public Guid Id { get; }
        public ITransport Transport { get; }
        public List<string> Subject { get; }

        public ReceiverConnection(Guid id, ITransport transport, List<string> subject)
        {
            Id = id;
            Transport = transport;
            Subject = subject;
        }
    }
}
