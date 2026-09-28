using db_service.Implementation;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using shared.Logging;
using System.Net;

IPAddress ip = IPAddress.Parse(GetEnv("BROKER_BIND_IP", "Give ip to bind broker to"));
int port = int.Parse(GetEnv("BROKER_BIND_PORT", "Give port to bind broker to"));
string dbPath = Environment.GetEnvironmentVariable("BROKER_DB_PATH") ?? "broker.db";
string logPath = Environment.GetEnvironmentVariable("BROKER_LOG_PATH") ?? "broker.log";
var logger = new FileLogger(logPath);

var options = new DbContextOptionsBuilder<BrokerDbContext>()
    .UseSqlite($"Data Source={dbPath}")
    .Options;

var contextFactory =
    new PooledDbContextFactory<BrokerDbContext>(options);

var messageService = new MessageService2(contextFactory);

await messageService.EnsureCreatedAsync();

var broker = new Broker.Broker(ip, port, messageService, logger);
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