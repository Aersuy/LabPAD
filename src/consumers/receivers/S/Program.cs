using receivers.S;
using shared.Logging;

string logPath = Environment.GetEnvironmentVariable("RECEIVER_LOG_PATH") ?? "receiver.log";

var logger = new FileLogger(logPath);
Receiver _receiver = new Receiver(logger);
_receiver.Start();
await _receiver.RunAsync();