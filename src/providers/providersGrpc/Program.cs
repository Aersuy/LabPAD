using providersGrpc;
using shared.Logging;

string logPath = Environment.GetEnvironmentVariable("SENDER_LOG_PATH") ?? "sender-grpc.log";

var logger = new FileLogger(logPath);

var sender = new GrpcSender(logger);
await sender.RunAsync();
