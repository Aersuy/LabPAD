using db_service.Interfaces;
using grpc_contracts;
using grpc_contracts.Mapping;
using Grpc.Core;
using shared.Logging;
using shared.Models;
using grpc_contracts.Identity;

namespace BrokerGrpc.Services
{
    // gRPC implementation of the broker, same logic as Broker from part 1
    // Every call is handled concurrently by ASP.NET Core (one task per call, HTTP/2 multiplexing)
    public class BrokerGrpcService : BrokerService.BrokerServiceBase
    {
        private readonly IMessageService2 _messageService;
        private readonly ReceiverRegistry _receivers;
        private readonly FileLogger _logger;

        private const int MaxAttempts = 5;
        private const int BatchSize = 10;
        private static readonly TimeSpan AckTimeout = TimeSpan.FromSeconds(5);
        private static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan PollInterval = TimeSpan.FromMilliseconds(500);

        public BrokerGrpcService(IMessageService2 messageService, ReceiverRegistry receivers, FileLogger logger)
        {
            _messageService = messageService;
            _receivers = receivers;
            _logger = logger;
        }

        // Validates the message, stores it if it is new and creates a delivery for every
        // subscribed receiver. The reply is the Ack, it is sent even if the message is a duplicate
        public override async Task<PublishReply> Publish(PublishRequest request, ServerCallContext context)
        {
            MessageEnvelope msg;
            try
            {
                msg = EnvelopeMapper.ToMessageEnvelope(request.Message);
            }
            catch (InvalidDataException ex)
            {
                _logger.Log("Broker", "Warning", $"Invalid message from {context.Peer}: {ex.Message}");
                throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
            }
            _logger.Log("Broker", "Info", $"Sender {msg.SenderId} sent {msg.MessageType} for subjects: {string.Join(", ", msg.Subject)}");
            IReadOnlyCollection<Guid> targetIds;
            try
            {
                targetIds = await _messageService.GetSubscriberIdsForSubjectsAsync(msg.Subject);
            }
            catch (Exception ex)
            {
                _logger.Log("Broker", "Error", $"Failed to get receivers for {msg.MessageId}: {ex.Message}");
                throw new RpcException(new Status(StatusCode.Unavailable, "Failed to get receivers"));
            }
            bool isNew;
            try
            {
                isNew = await _messageService.StoreMessageIfNewAsync(msg, targetIds);
            }
            catch (Exception ex)
            {
                _logger.Log("Broker", "Error", $"Store failed for {msg.MessageId}: {ex.Message}");
                // No ack if store fails, the sender will retry
                throw new RpcException(new Status(StatusCode.Unavailable, "Store failed"));
            }

            _logger.Log("Broker", "Info", $"Message {msg.MessageId}: {(isNew ? "stored" : "duplicate")}, {targetIds.Count} target receiver(s)");
            // Message is new but no subscribers, log a warning but still ack the sender
            if (isNew && targetIds.Count == 0)
            {
                _logger.Log("Broker", "Warning",
                    $"Message {msg.MessageId} has no subscriber for subjects: {string.Join(", ", msg.Subject)}");
            }
            if (isNew && targetIds.Count > 0)
            {
                _receivers.Notify(targetIds);
            }
            return new PublishReply
            {
                MessageAcknowledged = msg.MessageId.ToString(),
                Duplicate = !isNew,
                TargetReceivers = targetIds.Count
            };
        }

        // Keeps the stream open for as long as the receiver is connected
        // and pushes the due deliveries of this receiver from the storage
        public override async Task Subscribe(SubscribeRequest request, IServerStreamWriter<Delivery> responseStream, ServerCallContext context)
        {
            string name;
            Guid expectedId;
            try
            {
                name = ReceiverIdentity.NormalizeName(request.ReceiverName);
                expectedId = ReceiverIdentity.FromName(name);
            }
            catch (InvalidDataException ex)
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
            }

            Guid id = ParseId(request.ReceiverId, "ReceiverId");
            // Fail loudly instead of letting the acks of a mislabelled receiver go nowhere
            if (id != expectedId)
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, "ReceiverId does not match ReceiverName"));
            }

            List<string> subject = request.Subject
            .Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(s => s.Trim())
            .Distinct()
            .ToList();
            if (subject.Count == 0)
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, "At least one subject is required"));
            }

            try
            {
                await _messageService.UpsertSubscriptionAsync(id, name, subject);
            }
            catch (Exception ex)
            {
                _logger.Log("Broker", "Error", $"Failed to store subscription for {name}: {ex.Message}");
                throw new RpcException(new Status(StatusCode.Unavailable, "Failed to store subscription"));
            }

            using var subscription = new ReceiverSubscription(id, subject, context.CancellationToken);
            // Same receiver id subscribed again, close the old stream
            _receivers.Add(subscription)?.Cancel();
            _logger.Log("Broker", "Info", $"Receiver {id} registered for subjects : {string.Join(", ", subject)}");
            try
            {
                await ReceiverSendLoopAsync(subscription, responseStream, subscription.Cancellation.Token);
            }
            finally
            {
                _receivers.Remove(subscription);
                _logger.Log("Broker", "Info", $"Receiver {id} disconnected.");
            }
        }

        private async Task ReceiverSendLoopAsync(ReceiverSubscription receiver, IServerStreamWriter<Delivery> stream, CancellationToken ct)
        {
            
            var receiverIds = new[] { receiver.Id };
            try
            {
                while (!ct.IsCancellationRequested)
                {
                    bool batchWasFull = false;
                    try
                    {
                        var deliveries = await _messageService.GetDueDeliveriesAsync(DateTime.UtcNow, receiverIds, 10);
                        batchWasFull = deliveries.Count == BatchSize;
                        foreach (var delivery in deliveries)
                        {
                            ct.ThrowIfCancellationRequested();
                            if (delivery.Attempts >= MaxAttempts)
                            {
                                await _messageService.MarkDeadLetterAsync(delivery.MessageId, delivery.ReceiverId, "Max attempts");
                                continue;
                            }
                            var envelope = await _messageService.LoadEnvelopeAsync(delivery.MessageId);
                            if (envelope is null)
                                continue;
                            Envelope proto;
                            try
                            {
                                proto = EnvelopeMapper.ToProto(envelope);
                            }
                            catch (InvalidDataException ex)
                            {
                                await _messageService.MarkDeadLetterAsync(delivery.MessageId, delivery.ReceiverId, ex.Message);
                                continue;
                            }
                            var now = DateTime.UtcNow;
                            int updated = await _messageService.RecordAttemptAsync(
                                delivery.MessageId, delivery.ReceiverId, now, now + AckTimeout);
                            if (updated != 1)
                            {
                                continue;
                            }
                            using var sendTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                            sendTimeoutCts.CancelAfter(SendTimeout);
                            try
                            {
                                await stream.WriteAsync(new Delivery { Message = proto, Attempt = delivery.Attempts + 1 }, sendTimeoutCts.Token);
                                _logger.Log("Broker", "Info", $"Sent {envelope.MessageId} to {delivery.ReceiverId} (attempt {delivery.Attempts + 1})");
                            }
                            //Catch timeout exception
                            catch (OperationCanceledException) when (!ct.IsCancellationRequested)
                            {
                                _logger.Log("Broker", "Warning", $"Send to {delivery.ReceiverId} timed out");
                                return;
                            }
                            //Catch other exceptions
                            catch (Exception ex) when (!ct.IsCancellationRequested)
                            {
                                _logger.Log("Broker", "Warning", $"Send to {delivery.ReceiverId} failed: {ex.Message}");
                                return;
                            }
                        }
                    }
                    catch (OperationCanceledException) when (ct.IsCancellationRequested)
                    {
                        return;
                    }
                    catch (Exception ex) when (!ct.IsCancellationRequested)
                    {
                        // Database error
                        _logger.Log("Broker", "Error", $"Dispatcher error for {receiver.Id}: {ex.GetType().Name}: {ex.Message}");
                    }
                    if (batchWasFull)
                    {
                        continue;
                    }
                    await receiver.WaitForWorkAsync(PollInterval, ct);
                }
            }
            catch (OperationCanceledException)
            {
                // Normal
            }
        }

        public override async Task<AckReply> Acknowledge(AckRequest request, ServerCallContext context)
        {
            Guid receiverId = ParseId(request.ReceiverId, "ReceiverId");
            Guid messageId = ParseId(request.MessageAcknowledged, "MessageAcknowledged");
            _logger.Log("Broker", "Info", $"Receiver {receiverId} ACKed {messageId}");

            int updated = await RecordAsync(() => _messageService.MarkAckedAsync(messageId, receiverId), "Ack", receiverId);
            return new AckReply { Recorded = updated > 0 };
        }

        public override async Task<AckReply> Reject(NackRequest request, ServerCallContext context)
        {
            Guid receiverId = ParseId(request.ReceiverId, "ReceiverId");
            Guid messageId = ParseId(request.MessageNacked, "MessageNacked");
            _logger.Log("Broker", "Warning", $"Receiver {receiverId} NACKed {messageId} with reason: {request.Reason}, retryable: {request.Retryable}");

            int updated = request.Retryable
                ? await RecordAsync(() => _messageService.MarkRetryLaterAsync(messageId, receiverId, request.Reason, DateTime.UtcNow + TimeSpan.FromSeconds(5)), "Nack", receiverId)
                : await RecordAsync(() => _messageService.MarkDeadLetterAsync(messageId, receiverId, request.Reason), "Nack", receiverId);
            return new AckReply { Recorded = updated > 0 };
        }

        private static Guid ParseId(string value, string name)
        {
            try
            {
                return EnvelopeMapper.ParseId(value, name);
            }
            catch (InvalidDataException ex)
            {
                throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
            }
        }

        private async Task<int> RecordAsync(Func<Task<int>> action, string type, Guid receiverId)
        {
            try
            {
                return await action();
            }
            catch (Exception ex)
            {
                _logger.Log("Broker", "Error", $"Failed to record {type} from receiver {receiverId}: {ex.GetType().Name}: {ex.Message}");
                throw new RpcException(new Status(StatusCode.Unavailable, $"Failed to record {type}"));
            }
        }

        public override async Task<AckReply> Extend(ExtendRequest request, ServerCallContext context)
        {
            Guid receiverId = ParseId(request.ReceiverId, "ReceiverId");
            Guid messageId = ParseId(request.MessageId, "MessageId");

            int updated = await RecordAsync(
                () => _messageService.ExtendLeaseAsync(messageId, receiverId, DateTime.UtcNow + AckTimeout),
                "Extend", receiverId);
            return new AckReply { Recorded = updated > 0 };
        }
    }
}
