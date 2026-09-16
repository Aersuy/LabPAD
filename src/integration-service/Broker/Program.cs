using Broker;
using Broker.Db;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System.Net;

IPAddress ip = IPAddress.Parse(GetEnv("BROKER_BIND_IP", "Give ip to bind broker to"));
int port = int.Parse(GetEnv("BROKER_BIND_PORT", "Give port to bind broker to"));
string dbPath = Environment.GetEnvironmentVariable("BROKER_DB_PATH") ?? "broker.db";

var services = new ServiceCollection();
services.AddDbContextFactory<BrokerDbContext>(options => options.UseSqlite($"Data Source={dbPath}"));
services.AddSingleton<IMessageRepository, MessageRepository>();

await using ServiceProvider provider = services.BuildServiceProvider();
var messageRepository = provider.GetRequiredService<IMessageRepository>();
await messageRepository.EnsureCreatedAsync();

var broker = new Broker.Broker(ip, port, messageRepository);
broker.Start();
await broker.RunAsync();

static string GetEnv(string name, string prompt)
{
    string? env = Environment.GetEnvironmentVariable(name);
    if (!string.IsNullOrWhiteSpace(env))
    {
        return env;
    }
    Console.WriteLine(prompt);
    return Console.ReadLine()!;
}