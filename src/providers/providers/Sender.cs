using shared;
using shared.Enums;
using shared.IImplementations;
using shared.Interfaces;
using shared.Logging;
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
        private readonly FileLogger _logger;

        public Sender(FileLogger logger)
        {
            _logger = logger;
        }

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
                _logger.Log("Sender","Warning", $"Registration failed: {ex.Message}");
                return false;
            }
            if (response is null)
            {
                return false;
            }
            if (response.MessageType == MessageType.Error)
            {
                string reason = response.Subject.Count > 0 ? response.Subject[0] : "Unknown";
                _logger.Log("Sender", "Warning", $"Broker rejected registration: {reason} - {response.JsonPayload.GetRawText()}");
                return false;
            }
            if (response.MessageType != MessageType.Ack)
            {
                return false;
            }
            return true;
        }
        private volatile bool _connected;
        private readonly SemaphoreSlim _reconnectLock = new(1, 1);
        private async Task<bool> EnsureConnectedAsync()
        {
            if (_connected) return true;
            await _reconnectLock.WaitAsync();   
            try
            {
                if (_connected) return true;
                _socket?.Close();
                while (true)
                {
                    if (await RegisterWithBroker())
                    {
                        _connected = true;
                        _ = ReceiveLoopAsync();
                        return true;
                    }
                    _logger.Log("Sender", "Warning", "Reconnect failed,retry");
                    await Task.Delay(TimeSpan.FromSeconds(3));

                }
            } finally
            {
                _reconnectLock.Release();
            }
        }
        public async Task RunAsync()
        {
            if (!await EnsureConnectedAsync())
            {
                _logger.Log("Sender", "Error", "Failed to register");
                return;
            }
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
                    _logger.Log("Sender", "Warning", $"ReceiveLoopAsync threw: {ex.GetType().Name}: {ex.Message}");
                    break;
                }
                if (message is null)
                {
                    _logger.Log("Sender", "Warning", "Broker disconnected.");
                    break;
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
                    _logger.Log("Sender", "Warning", $"Failed to deserialize Ack from broker: {ex.Message}");
                }
            }
            _connected = false;
            foreach (var kvp in _pendingAcks)
            {
                kvp.Value.TrySetCanceled();
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
                if (!await EnsureConnectedAsync())
                {
                    _logger.Log("Sender", "Warning", "Failed to ensure connection before publish.");
                    return false;
                }

                var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                _pendingAcks[message.MessageId] = tcs;

                try
                {
                    await MessageProtocol.WriteMessageAsync(_transport!, message);
                }
                catch (Exception ex)
                {
                    _logger.Log("Sender","Warning", $"Send failed (attempt {attempt}): {ex.Message}");
                    _pendingAcks.TryRemove(message.MessageId, out _);
                    _connected = false;
                    await Task.Delay(TimeSpan.FromSeconds(1));
                    continue;
                }
                var winner = await Task.WhenAny(tcs.Task, Task.Delay(AckWaitTimeout));
                _pendingAcks.TryRemove(message.MessageId, out _);
                if (winner == tcs.Task && !tcs.Task.IsCanceled)
                {
                    _logger.Log("Sender", "Info", $"Message {message.MessageId} acknowledged (attempt {attempt}).");
                    return true;
                }
                _logger.Log("Sender","Warning", $"No ACK for {message.MessageId} within {AckWaitTimeout.TotalSeconds}s");
            }

            _logger.Log("Sender","Error", $"Giving up on {message.MessageId} after {MaxPublishAttempts} attempts");
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
                bool delivered = await PublishAsync(message);
                if (!delivered)
                {
                    _logger.Log("Sender","Warning", "Not delivered");
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
