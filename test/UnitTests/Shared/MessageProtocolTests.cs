using shared;
using shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnitTests.Helpers;

namespace UnitTests.Shared
{
    public class MessageProtocolTests
    {
        [Fact]
        public async Task WriteThenRead_RoundTrips()
        {
            var transport = new FakeTransport();
            var original = new MessageEnvelope
            {
                MessageId = Guid.NewGuid(),
                SenderId = Guid.NewGuid(),
                TimeStamp = DateTime.UtcNow,
                Subject = new List<string> { "temperature" },
                JsonPayload = System.Text.Json.JsonSerializer.SerializeToElement(new DataPayload { Content = "42" })
            };

            await MessageProtocol.WriteMessageAsync(transport, original);
            var result = await MessageProtocol.ReadMessageAsync(transport);

            Assert.NotNull(result);
            Assert.Equal(original.MessageId, result!.MessageId);
            Assert.Equal(original.Subject, result.Subject);
        }

        [Fact]
        public async Task Read_NoData_ReturnsNull()
        {
            var transport = new FakeTransport();

            var result = await MessageProtocol.ReadMessageAsync(transport);

            Assert.Null(result);
        }

        [Fact]
        public async Task Read_TruncatedLengthPrefix_ThrowsEndOfStreamException()
        {
            var transport = new FakeTransport();
            transport.EnqueueRaw(new byte[] { 1, 2 }); // only 2 of the required 4 bytes

            await Assert.ThrowsAsync<EndOfStreamException>(
                () => MessageProtocol.ReadMessageAsync(transport));
        }

        [Fact]
        public async Task Read_TruncatedPayload_ThrowsEndOfStreamException()
        {
            var transport = new FakeTransport();
            transport.EnqueueRaw(BitConverter.GetBytes(10)); // claims 10 bytes of payload
            transport.EnqueueRaw(new byte[] { 1, 2, 3 });     // but only 3 follow

            await Assert.ThrowsAsync<EndOfStreamException>(
                () => MessageProtocol.ReadMessageAsync(transport));
        }

        [Fact]
        public async Task Read_OversizedLength_ThrowsInvalidDataException()
        {
            var transport = new FakeTransport();
            transport.EnqueueRaw(BitConverter.GetBytes(2_000_000)); // >= MaxBytes (1_000_000)

            await Assert.ThrowsAsync<InvalidDataException>(
                () => MessageProtocol.ReadMessageAsync(transport));
        }

        [Fact]
        public async Task Read_ZeroOrNegativeLength_ThrowsInvalidDataException()
        {
            var transport = new FakeTransport();
            transport.EnqueueRaw(BitConverter.GetBytes(0));

            await Assert.ThrowsAsync<InvalidDataException>(
                () => MessageProtocol.ReadMessageAsync(transport));
        }

        [Fact]
        public async Task Read_MalformedJsonPayload_ThrowsInvalidDataException()
        {
            var transport = new FakeTransport();
            byte[] badJson = System.Text.Encoding.UTF8.GetBytes("not valid json");
            transport.EnqueueRaw(BitConverter.GetBytes(badJson.Length));
            transport.EnqueueRaw(badJson);

            await Assert.ThrowsAsync<InvalidDataException>(
                () => MessageProtocol.ReadMessageAsync(transport));
        }
    }
}
