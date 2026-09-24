using providers;
using shared.Logging;

string logPath = Environment.GetEnvironmentVariable("SENDER_LOG_PATH") ?? "sender.log";

var logger = new FileLogger(logPath);

var sender = new Sender(logger);
await sender.RunAsync();