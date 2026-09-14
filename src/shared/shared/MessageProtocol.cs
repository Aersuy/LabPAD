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

        public static async Task WriteMessageAsync(NetworkStream stream, MessageEnvelope msg)
        {
            byte[] payload = JsonSerializer.SerializeToUtf8Bytes(msg, JsonOptions);
            byte[] lengthPrefix = BitConverter.GetBytes(payload.Length);

            await stream.WriteAsync(lengthPrefix);
            await stream.WriteAsync(payload);
        }
        public static async Task<MessageEnvelope?> ReadMessageAsync(NetworkStream stream)
        {
            byte[] lengthBuffer = new byte[4];
            int read = await ReadExactAsync(stream, lengthBuffer, 4);

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
            int payloadRead = await ReadExactAsync(stream, payloadBuffer, length);
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
        private static async Task<int> ReadExactAsync(NetworkStream stream, byte[] buffer, int count)
        {
            int totalRead = 0;
            while (totalRead < count)
            {
                int n = await stream.ReadAsync(buffer, totalRead, count - totalRead);
                if (n == 0) { return totalRead; }
                totalRead += n;
            }
            return totalRead;
        }
    }
}
