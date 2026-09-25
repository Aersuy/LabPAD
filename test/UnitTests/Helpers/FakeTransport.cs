using shared.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
namespace UnitTests.Helpers
{
    public class FakeTransport : ITransport
    {
        private readonly Queue<byte> _buffer = new();
        public Task<int> sendAsync(ReadOnlyMemory<byte> data, CancellationToken ct = default)
        {
            foreach (var b in data.Span) _buffer.Enqueue(b);
            return Task.FromResult(data.Length);
        }

        public Task<int> receiveAsync(Memory<byte> data, CancellationToken ct = default)
        {
            int n = 0;
            while (n < data.Length && _buffer.Count > 0)
            {
                data.Span[n++] = _buffer.Dequeue();
            }
            return Task.FromResult(n);
        }

        public void close() { }

        // send bytes directly in queue
        public void EnqueueRaw(byte[] bytes)
        {
            foreach (var b in bytes) _buffer.Enqueue(b);
        }
    }
}
