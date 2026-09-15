using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace shared.Interfaces
{
    public interface ITransport
    {
        Task<int> sendAsync(ReadOnlyMemory<byte> data);
        Task<int> receiveAsync(Memory<byte> data);
        void close();
    }
}
