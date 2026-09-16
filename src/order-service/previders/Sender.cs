using shared;
using shared.Enums;
using shared.IImplementations;
using shared.Interfaces;
using shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace providers
{
    public class Sender
    {
        private Socket? _socket;
        private ITransport? _transport;
        private readonly Guid _id = Guid.NewGuid();

        private static readonly JsonSerializerOptions JsonOption = new()
        {
            Converters = { new JsonStringEnumConverter() }
        };

        public async Task<bool> RegisterWithBroker()
        {
            string host = GetBrokerHost();
            int port = GetBrokerPort();

            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

            await _socket.ConnectAsync(host, port);
            _transport = new VladTransport(_socket);

            var payload = new RegisterPayload
            {
                IpAddress = "0.0.0.0",
                Port = 0,
                Role = Roles.Sender

            };
            var registerMessage = new MessageEnvelope
            {
                MessageType = MessageType.Register,
                JsonPayload = JsonSerializer.SerializeToElement(payload, JsonOption),
                SenderId = _id,
                MessageId = Guid.NewGuid(),
                TimeStamp = DateTime.UtcNow,
            };
            await MessageProtocol.WriteMessageAsync(_transport, registerMessage);

            MessageEnvelope? response;
            try
            {
                response = await MessageProtocol.ReadMessageAsync(_transport);
            }
            catch (Exception ex) when (ex is EndOfStreamException or InvalidDataException)
            {
                Console.WriteLine($"Registration failed: {ex.Message}");
                return false;
            }
            if (response is null || response.MessageType != MessageType.Ack)
            {
                return false;
            }
            if (response.MessageType == MessageType.Error)
            {
                string reason = response.Subject.Count > 0 ? response.Subject[0] : "Unknown";
                Console.WriteLine($"Broker rejected registration: {reason} - {response.JsonPayload.GetRawText()}");
                return false;
            }

            return true;
        }

        public async Task RunAsync()
        {
            if (!await RegisterWithBroker())
            {
                Console.WriteLine("Failed to register");
                return;
            }
            await SendLoopAsync();
        }
        public async Task SendLoopAsync()
        {
            while (true)
            {
                List<string> subjects = GetSubjects();
                string content = GetContent();

                var payload = new DataPayload { Content = content };
                var message = new MessageEnvelope
                {
                    MessageType = MessageType.Data,
                    JsonPayload = JsonSerializer.SerializeToElement(payload, JsonOption),
                    SenderId = _id,
                    MessageId = Guid.NewGuid(),
                    TimeStamp = DateTime.UtcNow,
                    Subject = subjects
                };
                await MessageProtocol.WriteMessageAsync(_transport, message);

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
