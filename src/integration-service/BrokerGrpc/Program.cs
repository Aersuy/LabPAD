using BrokerGrpc.Services;
using db_service.Implementation;
using db_service.Interfaces;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using shared.Logging;
using System.Net;

IPAddress ip = IPAddress.Parse(GetEnv("BROKER_BIND_IP", "Give ip to bind broker to"));
int port = int.Parse(GetEnv("BROKER_BIND_PORT", "Give port to bind broker to"));
string dbPath = Environment.GetEnvironmentVariable("BROKER_DB_PATH") ?? "broker-grpc.db";
string logPath = Environment.GetEnvironmentVariable("BROKER_LOG_PATH") ?? "broker-grpc.log";
var logger = new FileLogger(logPath);

var options = new DbContextOptionsBuilder<BrokerDbContext>()
    .UseSqlite($"Data Source={dbPath}")
    .Options;

var contextFactory =
    new PooledDbContextFactory<BrokerDbContext>(options);

var messageService = new MessageService2(contextFactory);

await messageService.EnsureCreatedAsync();

var builder = WebApplication.CreateBuilder(args);
// gRPC runs over HTTP/2, plaintext (no TLS) so only Http2 is enabled on the endpoint
builder.WebHost.ConfigureKestrel(k => k.Listen(ip, port, o => o.Protocols = HttpProtocols.Http2));
builder.Logging.SetMinimumLevel(LogLevel.Warning);
builder.Services.AddGrpc(o =>
{
    o.MaxReceiveMessageSize = 2 * 1024 * 1024;
    o.EnableDetailedErrors = true;
});
builder.Services.AddSingleton(logger);
builder.Services.AddSingleton<IMessageService2>(messageService);
builder.Services.AddSingleton<ReceiverRegistry>();

var app = builder.Build();
app.MapGrpcService<BrokerGrpcService>();

logger.Log("Broker", "Info", $"gRPC broker listening on {ip}:{port}");
await app.RunAsync();

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
