using Google.Protobuf.WellKnownTypes;
using shared.Enums;
using shared.Models;
using System.Text;
using System.Text.Json;

namespace grpc_contracts.Mapping
{
    // Converts between the proto Envelope (wire format of part 2)
    // and shared.Models.MessageEnvelope (used by the db-service and the data handlers)
    // Every proto message coming from the network goes through ToMessageEnvelope,
    // which validates it and throws InvalidDataException if it is not valid
    public static class EnvelopeMapper
    {
        // Same limit as MessageProtocol.MaxBytes from part 1
        public const int MaxContentBytes = 1000000;

        public static MessageEnvelope ToMessageEnvelope(Envelope? message)
        {
            if (message is null)
            {
                throw new InvalidDataException("Message is missing");
            }
            Guid messageId = ParseId(message.MessageId, nameof(message.MessageId));
            Guid senderId = ParseId(message.SenderId, nameof(message.SenderId));

            if (message.TimeStamp is null)
            {
                throw new InvalidDataException("TimeStamp is missing");
            }
            if (message.Subject.Count == 0)
            {
                throw new InvalidDataException("At least one subject is required");
            }
            if (message.Subject.Any(string.IsNullOrWhiteSpace))
            {
                throw new InvalidDataException("Subjects can not be empty");
            }
            if (message.Version < 1)
            {
                throw new InvalidDataException($"Invalid version {message.Version}");
            }
            if (message.PayloadCase != Envelope.PayloadOneofCase.Data)
            {
                throw new InvalidDataException($"Unsupported payload {message.PayloadCase}");
            }
            if (Encoding.UTF8.GetByteCount(message.Data.Content) >= MaxContentBytes)
            {
                throw new InvalidDataException("Content too large");
            }

            return new MessageEnvelope
            {
                MessageType = MessageType.Data,
                MessageId = messageId,
                SenderId = senderId,
                TimeStamp = message.TimeStamp.ToDateTime(),
                Subject = message.Subject.Select(s => s.Trim()).Distinct().ToList(),
                Version = message.Version,
                JsonPayload = JsonSerializer.SerializeToElement(new shared.Models.DataPayload { Content = message.Data.Content })
            };
        }

        public static Envelope ToProto(MessageEnvelope message)
        {
            shared.Models.DataPayload? payload;
            try { payload = message.JsonPayload.Deserialize<shared.Models.DataPayload>(); }
            catch (Exception ex) when (ex is JsonException or InvalidOperationException)
            {
                throw new InvalidDataException("Stored payload is malformed", ex);
            }
            if (payload?.Content is null)
            {
                throw new InvalidDataException("Stored payload has no content");
            }

            var envelope = new Envelope
            {
                MessageId = message.MessageId.ToString(),
                SenderId = message.SenderId.ToString(),
                TimeStamp = Timestamp.FromDateTime(DateTime.SpecifyKind(message.TimeStamp, DateTimeKind.Utc)),
                Version = message.Version,
                Data = new DataPayload { Content = payload.Content }
            };
            envelope.Subject.AddRange(message.Subject);
            return envelope;
        }

        public static Guid ParseId(string? value, string name)
        {
            if (!Guid.TryParse(value, out Guid id) || id == Guid.Empty)
            {
                throw new InvalidDataException($"{name} is not a valid id");
            }
            return id;
        }
    }
}
