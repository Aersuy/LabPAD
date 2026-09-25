using receivers.Implementation;
using shared;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnitTests.Helpers;

namespace UnitTests.Shared
{
    public class MessageContractTests
    {
        private static string GetContractsDir()
        {
            var dir = new DirectoryInfo(AppContext.BaseDirectory);
            while (dir != null && !Directory.Exists(Path.Combine(dir.FullName, "contracts")))
            {
                dir = dir.Parent;
            }
            if (dir is null)
            {
                throw new DirectoryNotFoundException("Could not locate the 'contracts' folder from the test output directory.");
            }
            return Path.Combine(dir.FullName, "contracts");
        }

        private static async Task<shared.Models.MessageEnvelope?> LoadEnvelopeAsync(string fileName)
        {
            byte[] json = await File.ReadAllBytesAsync(Path.Combine(GetContractsDir(), fileName));
            var transport = new FakeTransport();
            transport.EnqueueRaw(BitConverter.GetBytes(json.Length));
            transport.EnqueueRaw(json);

            return await MessageProtocol.ReadMessageAsync(transport);
        }

        [Theory]
        [InlineData("valid-data.json", "Data")]
        [InlineData("valid-ack.json", "Ack")]
        [InlineData("valid-nack.json", "Nack")]
        [InlineData("valid-register.json", "Register")]
        public async Task ValidFixtures_RoundTripSuccessfully(string fileName, string expectedType)
        {
            var envelope = await LoadEnvelopeAsync(fileName);

            Assert.NotNull(envelope);
            Assert.Equal(expectedType, envelope!.MessageType.ToString());
        }

        [Fact]
        public async Task InvalidJson_ThrowsInvalidDataException()
        {
            await Assert.ThrowsAsync<InvalidDataException>(
                () => LoadEnvelopeAsync("invalid-data_json.json"));
        }

        [Fact]
        public async Task UnknownMessageType_ThrowsInvalidDataException()
        {
            await Assert.ThrowsAsync<InvalidDataException>(
                () => LoadEnvelopeAsync("invalid-unknown_type.json"));
        }

        [Fact]
        public async Task NullContent_EnvelopeParsesButHandlerRejectsIt()
        {
            var envelope = await LoadEnvelopeAsync("invalid-null-content.json");
            Assert.NotNull(envelope);

            var handler = new DataHandler1();
            Assert.Throws<PermanentFailureException>(() => handler.Handle(envelope!));
        }

        [Fact]
        public async Task MissingContentField_DefaultsToEmptyString_DoesNotThrow()
        {
            var envelope = await LoadEnvelopeAsync("invalid-missing-content.json");
            Assert.NotNull(envelope);

            var handler = new DataHandler1();
            var result = handler.Handle(envelope!);

            Assert.Equal(string.Empty, result);
        }
    }
}
