using receivers.Implementation;
using receivers.Interfaces;
using shared;
using shared.Enums;
using shared.IImplementations;
using shared.Interfaces;
using shared.Models;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;

namespace receivers.S
{
    public class Receiver
    {
        private Socket? _listenerSocket;
        private Socket? _brokerSocket;
        private ITransport? _transport;
        private Guid _id;
        private readonly Dictionary<int, IDataHandler> _dataHandlers = new()
        {
            [1] = new DataHandler1(),
        };  

        public void Start()
        {
            _id = Guid.NewGuid();
        }
       
        private List<string> GetSubjects()
        {
            Console.WriteLine("Give subjects to subscribe to (comma separated)");
            string? input = Console.ReadLine();

            return (input ?? string.Empty)
                .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
                .ToList();
        }
        public async Task<bool> RegisterWithBroker()
        {
            string brokerHost = GetBrokerHost();
            int brokerPort = GetBrokerPort();
            List<string> subjects = GetSubjects();

            _brokerSocket = new Socket(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp
            );

            Console.WriteLine($"Connecting to broker at {brokerHost}:{brokerPort}...");
            await _brokerSocket.ConnectAsync(brokerHost, brokerPort);
            Console.WriteLine("Connected to broker.");
            _transport = new VladTransport(_brokerSocket);

            var payload = new RegisterPayload
            {
                IpAddress = "0.0.0.0",
                Port = 0,
                Role = shared.Enums.Roles.Subscriber
            };
            var registerMessage = new MessageEnvelope
            {
                MessageType = shared.Enums.MessageType.Register,
                MessageId = Guid.NewGuid(),
                SenderId = _id,
                TimeStamp = DateTime.UtcNow,
                JsonPayload = System.Text.Json.JsonSerializer.SerializeToElement(payload),
                Subject = subjects
            };
            Console.WriteLine($"Registering with broker with subjects: {string.Join(", ", subjects)}");
            await MessageProtocol.WriteMessageAsync(
                _transport,
                registerMessage
            );
            Console.WriteLine("Waiting for broker response...");

            MessageEnvelope? response;

            try
            {
                response =
                    await MessageProtocol.ReadMessageAsync(
                        _transport
                    );
            }
            catch (Exception ex)
                when (ex is EndOfStreamException
                    or InvalidDataException)
            {
                Console.WriteLine(
                    $"Registration failed: {ex.Message}"
                );

                return false;
            }

            if (response is null)
            {
                return false;
            }

            if (response.MessageType != shared.Enums.MessageType.Ack)
            {
                Console.WriteLine($"Registration failed: expected Ack, got {response.MessageType}");
                return false;
            }
            Console.WriteLine("Registration confirmed.");
            return true;
        }
        public async Task RunAsync()
        {
            if (!await RegisterWithBroker())
            {
                Console.WriteLine("Failed to register");
                return;
            }
            await ReceiveLoopAsync();
        }
        private async Task ReceiveLoopAsync()
        {
            Console.WriteLine("Entering receive loop.");
            while (true)
            {
                Console.WriteLine("Waiting for next message from broker...");
                MessageEnvelope? message;
                try
                {
                    message = await MessageProtocol.ReadMessageAsync(_transport!);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ReceiveLoopAsync threw: {ex.GetType().Name}: {ex.Message}");
                    Console.WriteLine(ex.StackTrace);
                    return;
                }
                if (message is null)
                {
                    Console.WriteLine("Broker disconnected. (ReadMessageAsync returned null / 0 bytes read).");
                    return;
                }
                Console.WriteLine($"Received message: type={message.MessageType} id={message.MessageId}");
                try
                {
                    await HandleMessageAsync(message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"HandleMessage threw: {ex.GetType().Name}: {ex.Message}");
                    Console.WriteLine(ex.StackTrace);
                }
            }
        }
        private async Task HandleMessageAsync(MessageEnvelope message)
        {
            if (message.MessageType == shared.Enums.MessageType.Data)
            {
                await HandleDataAsync(message);
                return;
            }
            switch (message.MessageType)
            {
                case MessageType.Error:
                    Console.WriteLine(
                        $"[{message.TimeStamp:HH:mm:ss}] Error: {string.Join(", ", message.Subject)} - {message.JsonPayload.GetRawText()}"
                    );
                    break;

                case MessageType.Ack:
                    Console.WriteLine($"[{message.TimeStamp:HH:mm:ss}] Ack for {message.MessageId}");
                    break;

                default:
                    Console.WriteLine($"[{message.TimeStamp:HH:mm:ss}] Unhandled message type: {message.MessageType}");
                    break;
            }
        }
        private async Task HandleDataAsync(MessageEnvelope message)
        {
            // if the there is no handler for this message version send nack with no retry
            if(!_dataHandlers.TryGetValue(message.Version, out IDataHandler? handler))
            {
                await SendNackAsync(message, $"No handler for version {message.Version}", false);
                return;
            }
            try
            {
                handler.Handle(message);
            } catch (PermanentFailureException ex)
            {
                await SendNackAsync(message, ex.Message, false);
                return;
            } catch (Exception ex)
            {
                await SendNackAsync(message, ex.Message, true);
                return;
            }
            await SendAckAsync(message);
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

        private Task SendAckAsync(MessageEnvelope original) =>
          SendControlAsync(MessageType.Ack, new AckPayload { MessageAcknowledged = original.MessageId });
        private Task SendNackAsync(MessageEnvelope original, string reason,bool retryable) =>
          SendControlAsync(MessageType.Nack, new NackPayload { MessageNacked = original.MessageId, Reason = reason, Retryable = retryable });
        private Task SendControlAsync<T>(MessageType type, T payload)
        {
            var message = new MessageEnvelope
            {
                MessageType = type,
                MessageId = Guid.NewGuid(),
                SenderId = _id,
                TimeStamp = DateTime.UtcNow,
                JsonPayload = System.Text.Json.JsonSerializer.SerializeToElement(payload),
                Subject = new List<string>()
            };
            return MessageProtocol.WriteMessageAsync(_transport!, message);
        }
    }
}