using db_service.Interfaces;
using Microsoft.EntityFrameworkCore;
using shared;
using shared.Enums;
using shared.IImplementations;
using shared.Interfaces;
using shared.Models;
using System.Collections.Concurrent;
using System.Diagnostics;
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


        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
        };

        private readonly ConcurrentDictionary<Guid, ReceiverConnection> _receivers = new();

        public Broker(IPAddress ip, int port, IMessageService2 messageService)
        {
            _ip = ip;
            _port = port;
            _messageService = messageService;
        }
        public void Start()
        {
            _listenerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            _listenerSocket.Bind(new IPEndPoint(_ip, _port));
            _listenerSocket.Listen();

            Console.WriteLine("Broker listening");
        }
        public async Task RunAsync()
        {
            _ = DispatcherLoopAsync();
            while (true)
            {
                Socket clientSocket = await _listenerSocket!.AcceptAsync();
                _ = HandleClientAsync(clientSocket);
            }
        }
        private async Task HandleClientAsync(Socket socket)
        {
            ITransport transport = new VladTransport(socket);
            MessageEnvelope? registerMsg;
            try
            {
                registerMsg = await MessageProtocol.ReadMessageAsync(transport);
               
            } catch (Exception ex) when (ex is EndOfStreamException or InvalidDataException)
            {
                Console.WriteLine("Registration failed");
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
            } catch (JsonException ex)
            {
                await SendErrorMessageAsync(transport, "InvalidRegisterPayload", ex.Message);
                transport.close();
                return;
            }
            await SendAckAsync(transport, registerMsg.SenderId, registerMsg.MessageId);

            switch (payload.Role)
            {
                case Roles.Subscriber:
                    await HandleReceiverAsync(registerMsg.SenderId,transport,registerMsg.Subject);
                    break;
                case Roles.Sender:
                    await HandleSenderAsync(registerMsg.SenderId, transport);
                    break;
                default:
                    Console.WriteLine("Unrecognised role");
                    transport.close();
                    break;
            }
        }
        private static async Task SendErrorMessageAsync(ITransport transport  ,string message,string aditionalData = "")
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
                MessageId = Guid.NewGuid(),
                TimeStamp = DateTime.UtcNow,
                Subject = subject,
                JsonPayload = payload
            };
            await MessageProtocol.WriteMessageAsync(transport, msg);
        }
        private static async Task SendAckAsync(ITransport transport, Guid targetId, Guid messageAcknowledged)
        {
            var payload = JsonSerializer.SerializeToElement(new AckPayload { MessageAcknowledged = messageAcknowledged },JsonOptions);
            var ack = new MessageEnvelope
            {
                MessageType = MessageType.Ack,
                MessageId = Guid.NewGuid(),
                SenderId = targetId,
                TimeStamp = DateTime.UtcNow,
                JsonPayload = payload
            };
            await MessageProtocol.WriteMessageAsync (transport, ack);
        }
        private async Task HandleReceiverAsync(Guid id, ITransport transport,List<string> subject)
        {
            var connection = new ReceiverConnection(id, transport, subject);
            _receivers[id] = connection;

            Console.WriteLine($"Receiver {id} registered for subjects : {string.Join(", ", subject)}");

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
            catch (Exception ex) when (ex is EndOfStreamException or InvalidDataException)
            {
                Console.WriteLine($"Receiver {id} connection error: {ex.Message}");
            }
            finally
            {
                // Old version, remove the receiver from the dictionary and close the transport
                // Just removes th item with the id, connection doesn't matter
                //_receivers.TryRemove(id, out _);


                // Remove the reicever only if the connection matches, to avoid removing a new connection with the same id
                // Can't just check if the connection is the same because of atomicity, so we check and remove in one operation
                _receivers.TryRemove(new KeyValuePair<Guid, ReceiverConnection>(id, connection));
                transport.close();
                Console.WriteLine($"Receiver {id} disconnected.");
            }
        }

        private async Task HandleReceiverMessageAsync(Guid receiverId, MessageEnvelope msg)
        {
            try
            {
                switch(msg.MessageType)
                {
                    case MessageType.Ack:
                        var ack = msg.JsonPayload.Deserialize<AckPayload>(JsonOptions);
                        if (ack is null)
                        {
                            break;
                        }
                        Console.WriteLine($"Receiver {receiverId} ACKed {ack.MessageAcknowledged}");
                        await _messageService.MarkAckedAsync(ack.MessageAcknowledged, receiverId);
                        break;
                    case MessageType.Nack:
                        var nack = msg.JsonPayload.Deserialize<NackPayload>(JsonOptions);
                        if(nack is null)
                        {
                            break;
                        }
                        Console.WriteLine($"Receiver {receiverId} NACKed {nack.MessageNacked} with reason: {nack.Reason}, retryable: {nack.Retryable}");
                        if (nack.Retryable)
                        {
                            await _messageService.MarkRetryLaterAsync(nack.MessageNacked, receiverId,nack.Reason,DateTime.UtcNow + TimeSpan.FromSeconds(5));
                        } else
                        {
                            await _messageService.MarkDeadLetterAsync(nack.MessageNacked, receiverId, nack.Reason);
                        }
                        break;
                    default:
                        Console.WriteLine($"Receiver {receiverId} sent unrecognized message type: {msg.MessageType}");
                        break;
                }
            } 
            catch (JsonException ex)
            {
                Console.WriteLine($"Failed to deserialize message from receiver {receiverId}: {ex.Message}");
            }
            catch (Exception ex)
            {
                
                Console.WriteLine($"Failed to record {msg.MessageType} from receiver {receiverId}: {ex.GetType().Name}: {ex.Message}");
            }
        }

        // Handles the sender connection, checks for new messages,
        // stores them if they are new, sends an acknowledgment back to the sender even if
        // the message is a duplicate, dispatches to all receivers that are subscribed
        private async Task HandleSenderAsync(Guid id, ITransport transport)
        {
            Console.WriteLine($"Sender {id} registered");
            try
            {
                while (true)
                {
                    MessageEnvelope? msg = await MessageProtocol.ReadMessageAsync(transport);
                    if (msg is null)
                    {
                        break;
                    }
                    Console.WriteLine($"Sender {id} sent {msg.MessageType} for subjects: {string.Join(", ", msg.Subject)}");
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
                            Console.WriteLine($"Store failed for {msg.MessageId}: {ex.Message}");
                            continue;     // Skip sending ack if store fails
                        }

                        Console.WriteLine($"Message {msg.MessageId}: {(isNew ? "stored" : "duplicate")}, {targetIds.Count} target receiver(s)");
                        await SendAckAsync(transport, id, msg.MessageId);
                    }
                }

            } catch (Exception ex) when (ex is EndOfStreamException or InvalidDataException)
            {
                Console.Write($"Sender {id} connection error");
            }
            finally
            {
                transport.close();
                Console.WriteLine($"Sender {id} disconnected");
            }
        }
        private IReadOnlyCollection<Guid> GetReceiverIdsForSubjects(MessageEnvelope msg) =>
         _receivers.Values
        .Where(r => r.Subject.Intersect(msg.Subject).Any())
        .Select(r => r.Id)
        .ToList();
      
        private const int MaxAttempts = 5;
        private static readonly TimeSpan AckTimeout = TimeSpan.FromSeconds(5);

        private async Task DispatcherLoopAsync()
        {
            while (true)
            {
                try
                {
                    var deliveries = await _messageService.GetDueDeliveriesAsync(
                        DateTime.UtcNow, _receivers.Keys.ToArray(), 10);

                    foreach (var delivery in deliveries)
                    {
                        if (delivery.Attempts >= MaxAttempts)
                        {
                            await _messageService.MarkDeadLetterAsync(delivery.MessageId, delivery.ReceiverId, "Max attempts reached");
                            Console.WriteLine($"Dead-lettered {delivery.MessageId} for {delivery.ReceiverId}: max attempts");
                            continue;
                        }

                        if (!_receivers.TryGetValue(delivery.ReceiverId, out var receiver))
                            continue;                       

                        var envelope = await _messageService.LoadEnvelopeAsync(delivery.MessageId);
                        if (envelope is null)
                            continue;

                        await _messageService.RecordAttemptAsync(
                            delivery.MessageId, delivery.ReceiverId, DateTime.UtcNow + AckTimeout);

                        try
                        {
                            await MessageProtocol.WriteMessageAsync(receiver.Transport, envelope);
                            Console.WriteLine($"Sent {envelope.MessageId} to {delivery.ReceiverId} (attempt {delivery.Attempts + 1})");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Send to {delivery.ReceiverId} failed: {ex.Message}");
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Dispatcher error: {ex.GetType().Name}: {ex.Message}");
                }

                await Task.Delay(500);
            }
        }
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
