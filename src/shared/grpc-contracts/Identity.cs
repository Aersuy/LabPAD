using System.Security.Cryptography;
using System.Text;

namespace grpc_contracts.Identity
{
    // receivers identified by username
    // name always produces the same id
    // RFC 4122 version 5 (name based, SHA-1). SHA-1 is used as a hash here, not for security.
    public static class ReceiverIdentity
    {

        // namespace is a random GUID, changing it will change all generated it's
        private static readonly Guid Namespace = new("6b1f0f4c-0f3c-4f2a-9c1d-2d9a6a1b7f10");

        public const int MaxNameLength = 64;

        // name is lowercase and trimmed
        public static string NormalizeName(string? name)
        {
            string normalized = (name ?? string.Empty).Trim().ToLowerInvariant();
            if (normalized.Length == 0)
            {
                throw new InvalidDataException("Receiver name is required");
            }
            if (normalized.Length > MaxNameLength)
            {
                throw new InvalidDataException($"Receiver name is longer than {MaxNameLength} characters");
            }
            return normalized;
        }

        // generates a deterministic GUID based on the normalized name and the namespace
        public static Guid FromName(string? name)
        {
            byte[] nameBytes = Encoding.UTF8.GetBytes(NormalizeName(name));
            byte[] namespaceBytes = Namespace.ToByteArray(bigEndian: true);

            byte[] buffer = new byte[namespaceBytes.Length + nameBytes.Length];
            namespaceBytes.CopyTo(buffer, 0);
            nameBytes.CopyTo(buffer, namespaceBytes.Length);

            Span<byte> id = SHA1.HashData(buffer).AsSpan(0, 16);
            id[6] = (byte)((id[6] & 0x0F) | 0x50); // version 5
            id[8] = (byte)((id[8] & 0x3F) | 0x80); // RFC 4122 variant
            return new Guid(id, bigEndian: true);
        }
    }
}