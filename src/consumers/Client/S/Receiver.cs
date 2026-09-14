using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace receivers.S
{
    public class Receiver
    {
        private TcpListener? _listener;
        private static readonly IPAddress TestIp = IPAddress.Parse("127.0.0.1");
        const int TestPort = 6001;
        private void Start()
        {
            _listener = new TcpListener(TestIp, TestPort);
            _listener.Start();
            Console.WriteLine($"Receiver listening on {TestPort}...");
        }
        public async Task RegisterWithBroker()
        {
            Console.WriteLine("Give broker ip");
            IPAddress brokerIp = IPAddress.Parse(Console.ReadLine()!);
            Console.WriteLine("Give broker port");
            int BrokerPort = int.Parse(Console.ReadLine()!);
            var Endpoint = new IPEndPoint(brokerIp, BrokerPort);
            using TcpClient client = new();
            await client.ConnectAsync(Endpoint);
        }
        // the main loop is in the MainThread file
    }
}
