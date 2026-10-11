using receiversGrpc;
using shared.Logging;

string logPath = Environment.GetEnvironmentVariable("RECEIVER_LOG_PATH") ?? "receiver-grpc.log";

var logger = new FileLogger(logPath);
GrpcReceiver _receiver = new GrpcReceiver(logger);
_receiver.Start();
await _receiver.RunAsync();
