using db_service.Implementation;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using System.Net;

IPAddress ip = IPAddress.Parse(GetEnv("BROKER_BIND_IP", "Give ip to bind broker to"));
int port = int.Parse(GetEnv("BROKER_BIND_PORT", "Give port to bind broker to"));
string dbPath = Environment.GetEnvironmentVariable("BROKER_DB_PATH") ?? "broker.db";

var services = new ServiceCollection();
services.AddDbContextFactory<BrokerDbContext>(options => options.UseSqlite($"Data Source={dbPath}"));
services.AddSingleton<MessageService>();

await using ServiceProvider provider = services.BuildServiceProvider();
var messageService = provider.GetRequiredService<MessageService>();
await messageService.EnsureCreatedAsync();

var broker = new Broker.Broker(ip, port, messageService);
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