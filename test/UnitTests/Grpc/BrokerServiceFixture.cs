using BrokerGrpc.Services;
using db_service.Implementation;
using db_service.Interfaces;
using Grpc.Net.Client;
using grpc_contracts;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using shared.Logging;

namespace UnitTests.Grpc
{
    // Porneste brokerul gRPC real in proces, peste TestServer: aceleasi servicii ca in
    // BrokerGrpc/Program.cs, dar fara port si cu o baza SQLite temporara per test
    public sealed class BrokerServerFixture : IAsyncDisposable
    {
        private readonly IHost _host;
        private readonly string _dbPath;
        private readonly string _logPath;

        public GrpcChannel Channel { get; }
        public BrokerService.BrokerServiceClient Client { get; }
        public IMessageService2 Messages { get; }

        private BrokerServerFixture(IHost host, GrpcChannel channel, IMessageService2 messages, string dbPath, string logPath)
        {
            _host = host;
            _dbPath = dbPath;
            _logPath = logPath;
            Channel = channel;
            Messages = messages;
            Client = new BrokerService.BrokerServiceClient(channel);
        }

        public static async Task<BrokerServerFixture> StartAsync()
        {
            string id = Guid.NewGuid().ToString("N");
            string dbPath = Path.Combine(Path.GetTempPath(), $"broker-test-{id}.db");
            string logPath = Path.Combine(Path.GetTempPath(), $"broker-test-{id}.log");

            var options = new DbContextOptionsBuilder<BrokerDbContext>()
                .UseSqlite($"Data Source={dbPath}")
                .Options;
            var messageService = new MessageService2(new PooledDbContextFactory<BrokerDbContext>(options));
            await messageService.EnsureCreatedAsync();

            IHost host = await new HostBuilder()
                .ConfigureWebHost(web =>
                {
                    web.UseTestServer();
                    web.ConfigureServices(services =>
                    {
                        services.AddRouting();
                        services.AddGrpc();
                        services.AddSingleton(new FileLogger(logPath));
                        services.AddSingleton<IMessageService2>(messageService);
                        services.AddSingleton<ReceiverRegistry>();
                    });
                    web.Configure(app =>
                    {
                        app.UseRouting();
                        app.UseEndpoints(e => e.MapGrpcService<BrokerGrpcService>());
                    });
                })
                .StartAsync();

            HttpClient http = host.GetTestClient();
            var channel = GrpcChannel.ForAddress(http.BaseAddress!, new GrpcChannelOptions { HttpClient = http });
            return new BrokerServerFixture(host, channel, messageService, dbPath, logPath);
        }

        public async ValueTask DisposeAsync()
        {
            Channel.Dispose();
            await _host.StopAsync();
            _host.Dispose();
            Delete(_dbPath);
            Delete(_logPath);
        }

        private static void Delete(string path)
        {
            try { File.Delete(path); }
            catch (IOException) { }
        }
    }
}