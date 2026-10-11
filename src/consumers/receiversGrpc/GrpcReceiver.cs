using grpc_contracts;
using grpc_contracts.Mapping;
using Grpc.Core;
using Grpc.Net.Client;
using receivers.Implementation;
using receivers.Interfaces;
using shared.Logging;
using shared.Models;

namespace receiversGrpc
{
    // Part 2 receiver, subscribes to the broker with the Subscribe rpc (server stream)
    // and confirms every delivery with the Acknowledge / Reject rpc
    public class GrpcReceiver
    {
        private GrpcChannel? _channel;
        private BrokerService.BrokerServiceClient? _client;
        private Guid _id;
        private readonly IEffectStore _effectStore = new EffectStore(GetEffectsPath());
        private readonly Dictionary<int, IDataHandler> _dataHandlers = new()
        {
            [1] = new DataHandler1(),
        };
        private readonly FileLogger _logger;

        private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(3);
        private static readonly TimeSpan ControlTimeout = TimeSpan.FromSeconds(5);

        public GrpcReceiver(FileLogger logger)
        {
            _logger = logger;
        }
        private static string GetEffectsPath() =>
          Environment.GetEnvironmentVariable("RECEIVER_EFFECTS_PATH") ?? "effects-grpc.log";
        public void Start()
        {
            _id = Guid.CreateVersion7();
        }

        private List<string> GetSubjects()
        {
            Console.WriteLine("Give subjects to subscribe to (comma separated)");
            string? input = Console.ReadLine();

            return (input ?? string.Empty)
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToList();
        }
        public async Task RunAsync()
        {
            string brokerHost = GetBrokerHost();
            int brokerPort = GetBrokerPort();
            List<string> subjects = GetSubjects();

            _channel = GrpcChannel.ForAddress($"http://{brokerHost}:{brokerPort}");
            _client = new BrokerService.BrokerServiceClient(_channel);

            var request = new SubscribeRequest { ReceiverId = _id.ToString() };
            request.Subject.AddRange(subjects);

            // The deliveries are stored by the broker for this receiver id,
            // so after a reconnect the messages that were not acked are sent again
            while (true)
            {
                try
                {
                    _logger.Log("Receiver", "Info", $"Subscribing to broker at {brokerHost}:{brokerPort} with subjects: {string.Join(", ", subjects)}");
                    await ReceiveLoopAsync(request);
                    _logger.Log("Receiver", "Warning", "Broker closed the stream.");
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.InvalidArgument)
                {
                    _logger.Log("Receiver", "Error", $"Failed to register: {ex.Status.Detail}");
                    return;
                }
                catch (RpcException ex)
                {
                    _logger.Log("Receiver", "Warning", $"Broker disconnected: {ex.StatusCode} {ex.Status.Detail}");
                }
                await Task.Delay(ReconnectDelay);
            }
        }
        private async Task ReceiveLoopAsync(SubscribeRequest request)
        {
            using var call = _client!.Subscribe(request);
            _logger.Log("Receiver", "Info", "Entering receive loop.");
            await foreach (Delivery delivery in call.ResponseStream.ReadAllAsync())
            {
                try
                {
                    await HandleDeliveryAsync(delivery);
                }
                catch (Exception ex)
                {
                    _logger.Log("Receiver", "Error", $"HandleDelivery threw: {ex.GetType().Name}: {ex.Message}");
                }
            }
        }
        private async Task HandleDeliveryAsync(Delivery delivery)
        {
            MessageEnvelope message;
            try
            {
                message = EnvelopeMapper.ToMessageEnvelope(delivery.Message);
            }
            catch (InvalidDataException ex)
            {
                // Can not nack without a valid id, the broker will dead letter it after max attempts
                if (Guid.TryParse(delivery.Message?.MessageId, out _))
                {
                    await SendNackAsync(delivery.Message!.MessageId, ex.Message, false);
                }
                _logger.Log("Receiver", "Warning", $"Invalid delivery: {ex.Message}");
                return;
            }
            await HandleDataAsync(message);
        }
        private async Task HandleDataAsync(MessageEnvelope message)
        {
            string messageId = message.MessageId.ToString();
            // if the there is no handler for this message version send nack with no retry
            if (!_dataHandlers.TryGetValue(message.Version, out IDataHandler? handler))
            {
                await SendNackAsync(messageId, $"No handler for version {message.Version}", false);
                return;
            }
            try
            {
                string content = handler.Handle(message);
                bool applied = _effectStore.ApplyOnce(message.MessageId, content);
                if (applied)
                {
                    _logger.Log("Receiver", "Info", $"[{message.TimeStamp:HH:mm:ss}] (v{message.Version}) applied {message.MessageId}: {content}");
                    _logger.Log("Receiver", "Info", "Waiting for next message");
                }
            }
            catch (PermanentFailureException ex)
            {
                await SendNackAsync(messageId, ex.Message, false);
                return;
            }
            catch (Exception ex)
            {
                await SendNackAsync(messageId, ex.Message, true);
                return;
            }
            await SendAckAsync(messageId);
        }
        private static string GetBrokerHost()
        {
            Console.WriteLine("Give broker ip/host");
            return Console.ReadLine()!;
        }

        private static int GetBrokerPort()
        {
            Console.WriteLine("Give broker port");
            return int.Parse(Console.ReadLine()!);
        }

        // If the ack is lost the broker sends the message again after the ack timeout,
        // the effect store makes sure it is applied only once
        private async Task SendAckAsync(string messageId)
        {
            try
            {
                await _client!.AcknowledgeAsync(
                    new AckRequest { ReceiverId = _id.ToString(), MessageAcknowledged = messageId },
                    deadline: DateTime.UtcNow + ControlTimeout);
            }
            catch (RpcException ex)
            {
                _logger.Log("Receiver", "Warning", $"Ack for {messageId} failed: {ex.StatusCode}");
            }
        }
        private async Task SendNackAsync(string messageId, string reason, bool retryable)
        {
            try
            {
                await _client!.RejectAsync(
                    new NackRequest { ReceiverId = _id.ToString(), MessageNacked = messageId, Reason = reason, Retryable = retryable },
                    deadline: DateTime.UtcNow + ControlTimeout);
            }
            catch (RpcException ex)
            {
                _logger.Log("Receiver", "Warning", $"Nack for {messageId} failed: {ex.StatusCode}");
            }
        }
    }
}
