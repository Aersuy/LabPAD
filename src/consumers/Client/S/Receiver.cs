using shared;
using shared.Interfaces;
using shared.IImplementations;
using shared.Models;
using System.Net;
using System.Net.Sockets;

namespace receivers.S
{
    public class Receiver
    {
        private Socket? _listenerSocket;
        private Socket? _brokerSocket;
        private ITransport? _transport;
        private Guid _id;

        public void Start()
        {
            //_listenerSocket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            //_listenerSocket.Bind(new IPEndPoint(TestIp, TestPort));
            //_listenerSocket.Listen();
            _id = Guid.NewGuid();
        }

        public async Task<bool> RegisterWithBroker()
        {
            Console.WriteLine("Give broker ip");
            IPAddress brokerIp =
                IPAddress.Parse(Console.ReadLine()!);

            Console.WriteLine("Give broker port");
            int brokerPort =
                int.Parse(Console.ReadLine()!);

            var endpoint =
                new IPEndPoint(brokerIp, brokerPort);

            _brokerSocket = new Socket(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp
            );

            await _brokerSocket.ConnectAsync(endpoint);

            _transport = new VladTransport(_brokerSocket);

            var registerMessage = new MessageEnvelope
            {
                MessageType = shared.Enums.MessageType.Register,
                MessageId = Guid.NewGuid(),
                SenderId = _id,
                TimeStamp = DateTime.UtcNow,
            };

            await MessageProtocol.WriteMessageAsync(
                _transport,
                registerMessage
            );

            MessageEnvelope? response;

            try
            {
                response =
                    await MessageProtocol.ReadMessageAsync(
                        _transport
                    );
            }
            catch (Exception ex)
                when (ex is EndOfStreamException
                    or InvalidDataException)
            {
                Console.WriteLine(
                    $"Registration failed: {ex.Message}"
                );

                return false;
            }

            if (response is null)
            {
                return false;
            }

            if (response.MessageType !=
                shared.Enums.MessageType.Ack)
            {
                return false;
            }

            return true;
        }
        public async Task RunAsync()
        {
            if (!await RegisterWithBroker())
            {
                Console.WriteLine("Failed to register");
                return;
            }
            await ReceiveLoopAsync();
        }
        private async Task ReceiveLoopAsync()
        {
            while (true)
            {
                MessageEnvelope? message =
                    await MessageProtocol.ReadMessageAsync(_transport!);

                if (message is null)
                {
                    Console.WriteLine("Broker disconnected.");
                    return;
                }

                //HandleMessage(message);
            }
        }
    }
}