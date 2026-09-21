using shared.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace shared.IImplementations
{
    public class VladTransport : ITransport
    {
        private readonly Socket _socket;
        private readonly SemaphoreSlim _sendLock = new SemaphoreSlim(1, 1);
        public VladTransport(Socket socket) 
        {
            _socket = socket;
        }
        // need to write comment
        public async Task<int> sendAsync(ReadOnlyMemory<byte> data)
        {
            await _sendLock.WaitAsync();
            try
            {
                int total = 0;
                while(total < data.Length)
                {
                    total += await _socket.SendAsync(data.Slice(total), SocketFlags.None);
                }
                return total;
            }
            finally
            {
                _sendLock.Release();
            }
        }
        public async Task<int> receiveAsync(Memory<byte> buffer)
        {
            return await _socket.ReceiveAsync(buffer,SocketFlags.None);
        }
        public void close()
        {
            _socket.Close();
        }
    }
}
