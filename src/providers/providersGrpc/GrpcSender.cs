using Google.Protobuf.WellKnownTypes;
using grpc_contracts;
using Grpc.Core;
using Grpc.Net.Client;
using shared.Logging;

namespace providersGrpc
{
    // Part 2 sender, publishes to the broker with the Publish rpc
    // The PublishReply is the Ack of the broker, no reply in time means the message is sent again
    // with the same MessageId, the broker ignores duplicates
    public class GrpcSender
    {
        private GrpcChannel? _channel;
        private BrokerService.BrokerServiceClient? _client;
        private readonly Guid _id = Guid.CreateVersion7();
        private readonly FileLogger _logger;

        private const int MaxPublishAttempts = 5;
        private static readonly TimeSpan AckWaitTimeout = TimeSpan.FromSeconds(5);

        public GrpcSender(FileLogger logger)
        {
            _logger = logger;
        }

        public void Connect()
        {
            string host = GetBrokerHost();
            int port = GetBrokerPort();

            // The channel reconnects by itself if the broker goes down
            _channel = GrpcChannel.ForAddress($"http://{host}:{port}");
            _client = new BrokerService.BrokerServiceClient(_channel);
            _logger.Log("Sender", "Info", $"Using broker at {host}:{port}");
        }

        public async Task RunAsync()
        {
            Connect();
            await SendLoopAsync();
            _channel?.Dispose();
        }

        private async Task<bool> PublishAsync(PublishRequest request)
        {
            string messageId = request.Message.MessageId;
            for (int attempt = 0; attempt < MaxPublishAttempts; attempt++)
            {
                try
                {
                    PublishReply reply = await _client!.PublishAsync(request, deadline: DateTime.UtcNow + AckWaitTimeout);
                    _logger.Log("Sender", "Info", $"Message {reply.MessageAcknowledged} acknowledged (attempt {attempt}, {(reply.Duplicate ? "duplicate" : "new")}, {reply.TargetReceivers} target receiver(s)).");
                    if (reply.TargetReceivers == 0)
                    {
                        _logger.Log("Sender", "Warning",
                            $"Message {reply.MessageAcknowledged} has no subscriber, it will not be delivered");
                    }
                    return true;
                }
                // Message rejected by validation, sending it again will not help
                catch (RpcException ex) when (ex.StatusCode == StatusCode.InvalidArgument)
                {
                    _logger.Log("Sender", "Error", $"Broker rejected {messageId}: {ex.Status.Detail}");
                    return false;
                }
                catch (RpcException ex) when (ex.StatusCode == StatusCode.DeadlineExceeded)
                {
                    _logger.Log("Sender", "Warning", $"No ACK for {messageId} within {AckWaitTimeout.TotalSeconds}s");
                }
                catch (RpcException ex)
                {
                    _logger.Log("Sender", "Warning", $"Send failed (attempt {attempt}): {ex.StatusCode} {ex.Status.Detail}");
                    await Task.Delay(TimeSpan.FromSeconds(1));
                }
            }

            _logger.Log("Sender", "Error", $"Giving up on {messageId} after {MaxPublishAttempts} attempts");
            return false;
        }

        public async Task SendLoopAsync()
        {
            while (true)
            {
                List<string> subjects = GetSubjects();
                string content = GetContent();

                var message = new Envelope
                {
                    MessageId = Guid.CreateVersion7().ToString(),
                    SenderId = _id.ToString(),
                    TimeStamp = Timestamp.FromDateTime(DateTime.UtcNow),
                    Version = 1,
                    Data = new DataPayload { Content = content }
                };
                message.Subject.AddRange(subjects);

                bool delivered = await PublishAsync(new PublishRequest { Message = message });
                if (!delivered)
                {
                    _logger.Log("Sender", "Warning", "Not delivered");
                }

                Console.WriteLine("Send another message? (y/n)");

                string? again = Console.ReadLine();
                if (!string.Equals(again, "y", StringComparison.OrdinalIgnoreCase))
                {
                    break;
                }
            }
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
        private static string GetContent()
        {
            Console.WriteLine("Give content");
            return Console.ReadLine() ?? string.Empty;
        }
        private static List<string> GetSubjects()
        {
            Console.WriteLine("Give subjects");
            string? input = Console.ReadLine();

            return (input ?? string.Empty)
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToList();
        }
    }
}
