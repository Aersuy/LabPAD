using Broker;
using System.Net;

IPAddress ip = GetIp();
int port = GetPort();

var broker = new Broker.Broker(ip, port);
broker.Start();
await broker.RunAsync();

static IPAddress GetIp()
{
    string? env = Environment.GetEnvironmentVariable("BROKER_BIND_IP");
    if (!string.IsNullOrWhiteSpace(env))
    {
        return IPAddress.Parse(env);
    }

    Console.WriteLine("Give ip to bind broker to");
    return IPAddress.Parse(Console.ReadLine()!);
}

static int GetPort()
{
    string? env = Environment.GetEnvironmentVariable("BROKER_BIND_PORT");
    if (!string.IsNullOrWhiteSpace(env))
    {
        return int.Parse(env);
    }

    Console.WriteLine("Give port to bind broker to");
    return int.Parse(Console.ReadLine()!);
}