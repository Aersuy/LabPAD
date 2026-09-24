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

namespace shared
{
    public static class MessageProtocol
    {
        private static readonly int MaxBytes = 1000000;

        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            Converters = { new JsonStringEnumConverter() }
        };
        public static async Task WriteMessageAsync(ITransport stream, MessageEnvelope msg, CancellationToken ct = default)
        {
            // Old way, not really thread safe, multiple threads writing to the same stream can cause issues
            //byte[] payload = JsonSerializer.SerializeToUtf8Bytes(msg, JsonOptions);
            //byte[] lengthPrefix = BitConverter.GetBytes(payload.Length);

            //await stream.sendAsync(lengthPrefix);
            //await stream.sendAsync(payload);

            // new way, uses a single buffer to avoid multiple writes and potential thread safety issues
            byte[] payload = JsonSerializer.SerializeToUtf8Bytes(msg, JsonOptions);
            byte[] frame = new byte[4 + payload.Length];
            BitConverter.TryWriteBytes(frame.AsSpan(0, 4), payload.Length);
            payload.CopyTo(frame.AsSpan(4));
            await stream.sendAsync(frame,ct);
        }
        public static async Task<MessageEnvelope?> ReadMessageAsync(ITransport stream, CancellationToken ct = default)
        {
            byte[] lengthBuffer = new byte[4];
            int read = await ReadExactAsync(stream, lengthBuffer, 4,ct);

            if (read == 0) { return null; }
            if (read < 4)
            {
                throw new EndOfStreamException("Conectiunea sa inchis in timpul citiri prefixului");
            }
            int length = BitConverter.ToInt32(lengthBuffer, 0);

            if (length <= 0 || length >= MaxBytes)
            {
                throw new InvalidDataException("Lungimea nerezonabila");
            }

            byte[] payloadBuffer = new byte[length];
            int payloadRead = await ReadExactAsync(stream, payloadBuffer, length, ct);
            if (payloadRead < length)
            {
                throw new EndOfStreamException("Conexiunea sa inchis in timpul citiri payload-ului");
            }
            try
            {
                return JsonSerializer.Deserialize<MessageEnvelope?>(payloadBuffer, JsonOptions);
            }
            catch (JsonException ex)
            {
                throw new InvalidDataException($"Payload invalid: {ex.Message}");
            }
        }
        private static async Task<int> ReadExactAsync(ITransport stream, byte[] buffer, int count, CancellationToken ct = default)
        {
            int totalRead = 0;
            while (totalRead < count)
            {
                int n = await stream.receiveAsync(buffer.AsMemory(totalRead, count - totalRead), ct);
                if (n == 0) { return totalRead; }
                totalRead += n;
            }
            return totalRead;
        }
    }
}
