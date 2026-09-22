using shared;
using shared.Enums;
using shared.IImplementations;
using shared.Interfaces;
using shared.Models;
using System;
using System.Collections.Concurrent;
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
        private readonly Guid _id = Guid.CreateVersion7();
        private readonly ConcurrentDictionary<Guid, TaskCompletionSource> _pendingAcks = new();

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
                MessageId = Guid.CreateVersion7(),
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
            if (response is null)
            {
                return false;
            }
            if (response.MessageType == MessageType.Error)
            {
                string reason = response.Subject.Count > 0 ? response.Subject[0] : "Unknown";
                Console.WriteLine($"Broker rejected registration: {reason} - {response.JsonPayload.GetRawText()}");
                return false;
            }
            if (response.MessageType != MessageType.Ack)
            {
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
            _ = ReceiveLoopAsync();
            await SendLoopAsync();
        }
        // receives acks from the broker
        // if the messageId from the ack matches with the broker
        // we get a reference to tcs and set it to completed
        public async Task ReceiveLoopAsync()
        {
            while (true)
            {
                MessageEnvelope? message;
                try
                {
                    message = await MessageProtocol.ReadMessageAsync(_transport!);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"ReceiveLoopAsync threw: {ex.GetType().Name}: {ex.Message}");
                    return;
                }
                if (message is null)
                {
                    Console.WriteLine("Broker disconnected.");
                    return;
                }
                try
                {
                    var ack = message.JsonPayload.Deserialize<AckPayload>(JsonOption);
                    if (ack is not null && _pendingAcks.TryGetValue(ack.MessageAcknowledged, out var tcs))
                    {
                        tcs.TrySetResult();
                    }
                }
                catch (JsonException ex)
                {
                    Console.WriteLine($"Failed to deserialize Ack from broker: {ex.Message}");
                }
            }
        }
        private const int MaxPublishAttempts = 5;
        private static readonly TimeSpan AckWaitTimeout = TimeSpan.FromSeconds(5);
        // sends the message and waits for ack, if the reference to the
        // task in the _pendingAcks buffer is set to finished
        // the program understands it worked
        private async Task<bool> PublishAsync(MessageEnvelope message)
        {
            for (int attempt = 0; attempt < MaxPublishAttempts; attempt++)
            {
                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _pendingAcks[message.MessageId] = tcs;

                try
                {
                    await MessageProtocol.WriteMessageAsync(_transport!, message);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Send failed (attempt {attempt}): {ex.Message}");
                    _pendingAcks.TryRemove(message.MessageId, out _);
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    continue;
                }
                var winner = await Task.WhenAny(tcs.Task, Task.Delay(AckWaitTimeout));
                _pendingAcks.TryRemove(message.MessageId, out _);
                if (winner == tcs.Task)
                {
                    Console.WriteLine($"Message {message.MessageId} acknowledged (attempt {attempt}).");
                    return true;
                }
                Console.WriteLine($"No ACK for {message.MessageId} within {AckWaitTimeout.TotalSeconds}s");
            }
            Console.WriteLine($"Giving up on {message.MessageId} after {MaxPublishAttempts} attempts");
            return false;
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
                    MessageId = Guid.CreateVersion7(),
                    TimeStamp = DateTime.UtcNow,
                    Subject = subjects
                };
                await PublishAsync(message);

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
