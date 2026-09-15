using shared;
using shared.Enums;
using shared.IImplementations;
using shared.Interfaces;
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

        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            Converters = { new System.Text.Json.Serialization.JsonStringEnumConverter() }
        };

        private readonly ConcurrentDictionary<Guid, ReceiverConnection> _receivers = new();

        public Broker(IPAddress ip, int port)
        {
            _ip = ip;
            _port = port;
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
                    
                }
            }
            catch (Exception ex) when (ex is EndOfStreamException or InvalidDataException)
            {
                Console.WriteLine($"Receiver {id} connection error: {ex.Message}");
            }
            finally
            {
                _receivers.TryRemove(id, out _);
                transport.close();
                Console.WriteLine($"Receiver {id} disconnected.");
            }
        }
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
                    if (msg.MessageType == MessageType.Data)
                    {
                        await DispatchAsync(msg);
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
        private async Task DispatchAsync(MessageEnvelope msg)
        {
            List<ReceiverConnection> targets = _receivers.Values
                .Where(r => r.Subject.Intersect(msg.Subject).Any()).ToList();

            IEnumerable<Task> sendTasks = targets.Select(async r =>
            {
                try
                {
                    await MessageProtocol.WriteMessageAsync(r.Transport, msg);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed to deliver to receiver {r.Id}: {ex.Message}");
                    _receivers.TryRemove(r.Id, out _);
                }
            });
            await Task.WhenAll(sendTasks);
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
