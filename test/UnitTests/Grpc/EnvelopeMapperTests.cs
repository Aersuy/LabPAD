using Google.Protobuf.WellKnownTypes;
using grpc_contracts;
using grpc_contracts.Mapping;
using receivers.Implementation;
using shared.Enums;
using shared.Models;
using System.Text.Json;

namespace UnitTests.Grpc
{
    public class EnvelopeMapperTests
    {
        private static Envelope ValidEnvelope()
        {
            var envelope = new Envelope
            {
                MessageId = Guid.NewGuid().ToString(),
                SenderId = Guid.NewGuid().ToString(),
                TimeStamp = Timestamp.FromDateTime(DateTime.UtcNow),
                Version = 1,
                Data = new grpc_contracts.DataPayload { Content = "42" }
            };
            envelope.Subject.Add("temperature");
            return envelope;
        }

        [Fact]
        public void ToMessageEnvelope_Valid_MapsAllFields()
        {
            var proto = ValidEnvelope();

            MessageEnvelope result = EnvelopeMapper.ToMessageEnvelope(proto);

            Assert.Equal(MessageType.Data, result.MessageType);
            Assert.Equal(Guid.Parse(proto.MessageId), result.MessageId);
            Assert.Equal(Guid.Parse(proto.SenderId), result.SenderId);
            Assert.Equal(new List<string> { "temperature" }, result.Subject);
            Assert.Equal(1, result.Version);
            // The part 1 data handler can read the mapped payload
            Assert.Equal("42", new DataHandler1().Handle(result));
        }

        [Fact]
        public void ToProto_ThenBack_RoundTrips()
        {
            var original = new MessageEnvelope
            {
                MessageType = MessageType.Data,
                MessageId = Guid.NewGuid(),
                SenderId = Guid.NewGuid(),
                TimeStamp = DateTime.UtcNow,
                Subject = new List<string> { "a", "b" },
                JsonPayload = JsonSerializer.SerializeToElement(new shared.Models.DataPayload { Content = "hello" })
            };

            MessageEnvelope result = EnvelopeMapper.ToMessageEnvelope(EnvelopeMapper.ToProto(original));

            Assert.Equal(original.MessageId, result.MessageId);
            Assert.Equal(original.SenderId, result.SenderId);
            Assert.Equal(original.Subject, result.Subject);
            Assert.Equal(original.TimeStamp, result.TimeStamp);
            Assert.Equal("hello", new DataHandler1().Handle(result));
        }

        [Fact]
        public void ToMessageEnvelope_Null_ThrowsInvalidDataException()
        {
            Assert.Throws<InvalidDataException>(() => EnvelopeMapper.ToMessageEnvelope(null));
        }

        [Theory]
        [InlineData("")]
        [InlineData("not a guid")]
        [InlineData("00000000-0000-0000-0000-000000000000")]
        public void ToMessageEnvelope_InvalidMessageId_ThrowsInvalidDataException(string id)
        {
            var proto = ValidEnvelope();
            proto.MessageId = id;

            Assert.Throws<InvalidDataException>(() => EnvelopeMapper.ToMessageEnvelope(proto));
        }

        [Fact]
        public void ToMessageEnvelope_NoSubject_ThrowsInvalidDataException()
        {
            var proto = ValidEnvelope();
            proto.Subject.Clear();

            Assert.Throws<InvalidDataException>(() => EnvelopeMapper.ToMessageEnvelope(proto));
        }

        [Fact]
        public void ToMessageEnvelope_BlankSubject_ThrowsInvalidDataException()
        {
            var proto = ValidEnvelope();
            proto.Subject.Add("  ");

            Assert.Throws<InvalidDataException>(() => EnvelopeMapper.ToMessageEnvelope(proto));
        }

        [Fact]
        public void ToMessageEnvelope_MissingPayload_ThrowsInvalidDataException()
        {
            var proto = ValidEnvelope();
            proto.ClearPayload();

            Assert.Throws<InvalidDataException>(() => EnvelopeMapper.ToMessageEnvelope(proto));
        }

        [Fact]
        public void ToMessageEnvelope_MissingTimeStamp_ThrowsInvalidDataException()
        {
            var proto = ValidEnvelope();
            proto.TimeStamp = null;

            Assert.Throws<InvalidDataException>(() => EnvelopeMapper.ToMessageEnvelope(proto));
        }

        [Fact]
        public void ToMessageEnvelope_InvalidVersion_ThrowsInvalidDataException()
        {
            var proto = ValidEnvelope();
            proto.Version = 0;

            Assert.Throws<InvalidDataException>(() => EnvelopeMapper.ToMessageEnvelope(proto));
        }

        [Fact]
        public void ToMessageEnvelope_ContentTooLarge_ThrowsInvalidDataException()
        {
            var proto = ValidEnvelope();
            proto.Data.Content = new string('x', EnvelopeMapper.MaxContentBytes);

            Assert.Throws<InvalidDataException>(() => EnvelopeMapper.ToMessageEnvelope(proto));
        }

        [Fact]
        public void ToProto_NullContent_ThrowsInvalidDataException()
        {
            var stored = new MessageEnvelope
            {
                MessageId = Guid.NewGuid(),
                SenderId = Guid.NewGuid(),
                TimeStamp = DateTime.UtcNow,
                Subject = new List<string> { "a" },
                JsonPayload = JsonDocument.Parse("""{"Content": null}""").RootElement
            };

            Assert.Throws<InvalidDataException>(() => EnvelopeMapper.ToProto(stored));
        }
    }
}
