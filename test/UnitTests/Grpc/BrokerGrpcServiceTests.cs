using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using grpc_contracts;
using grpc_contracts.Identity;
using Xunit;

namespace UnitTests.Grpc
{
    public class BrokerGrpcServiceTests
    {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        private static PublishRequest NewPublish(string subject, string content, string? messageId = null)
        {
            var envelope = new Envelope
            {
                MessageId = messageId ?? Guid.CreateVersion7().ToString(),
                SenderId = Guid.CreateVersion7().ToString(),
                TimeStamp = Timestamp.FromDateTime(DateTime.UtcNow),
                Version = 1,
                Data = new DataPayload { Content = content }
            };
            envelope.Subject.Add(subject);
            return new PublishRequest { Message = envelope };
        }

        private static SubscribeRequest NewSubscribe(string name, string subject)
        {
            var request = new SubscribeRequest
            {
                ReceiverId = ReceiverIdentity.FromName(name).ToString(),
                ReceiverName = name
            };
            request.Subject.Add(subject);
            return request;
        }

        // Subscribe este asincron pe server, publicarea trebuie sa astepte abonamentul persistat
        private static async Task WaitForSubscriptionAsync(BrokerServerFixture broker, string subject)
        {
            using var cts = new CancellationTokenSource(Timeout);
            while (true)
            {
                var ids = await broker.Messages.GetSubscriberIdsForSubjectsAsync(new[] { subject });
                if (ids.Count > 0)
                {
                    return;
                }
                await Task.Delay(25, cts.Token);
            }
        }

        [Fact]
        public async Task Publish_InvalidEnvelope_ReturnsInvalidArgument()
        {
            await using var broker = await BrokerServerFixture.StartAsync();
            var request = new PublishRequest { Message = new Envelope { MessageId = "not-a-guid" } };

            RpcException ex = await Assert.ThrowsAsync<RpcException>(
                () => broker.Client.PublishAsync(request).ResponseAsync);

            Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        }

        [Fact]
        public async Task Publish_SameMessageTwice_SecondIsDuplicate()
        {
            await using var broker = await BrokerServerFixture.StartAsync();
            PublishRequest request = NewPublish("test", "hello");

            PublishReply first = await broker.Client.PublishAsync(request);
            PublishReply second = await broker.Client.PublishAsync(request);

            Assert.False(first.Duplicate);
            Assert.True(second.Duplicate);
            Assert.Equal(request.Message.MessageId, second.MessageAcknowledged);
        }

        [Fact]
        public async Task Subscribe_NameAndIdMismatch_ReturnsInvalidArgument()
        {
            await using var broker = await BrokerServerFixture.StartAsync();
            SubscribeRequest request = NewSubscribe("alice", "test");
            request.ReceiverId = Guid.CreateVersion7().ToString();

            using var call = broker.Client.Subscribe(request);
            RpcException ex = await Assert.ThrowsAsync<RpcException>(
                async () => await call.ResponseStream.MoveNext(CancellationToken.None));

            Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        }

        [Fact]
        public async Task Subscribe_ThenPublish_PushesDeliveryOnStream()
        {
            await using var broker = await BrokerServerFixture.StartAsync();
            using var cts = new CancellationTokenSource(Timeout);

            using var call = broker.Client.Subscribe(NewSubscribe("alice", "test"), cancellationToken: cts.Token);
            await WaitForSubscriptionAsync(broker, "test");

            await broker.Client.PublishAsync(NewPublish("test", "hello"));

            Assert.True(await call.ResponseStream.MoveNext(cts.Token));
            Delivery delivery = call.ResponseStream.Current;
            Assert.Equal("hello", delivery.Message.Data.Content);
            Assert.Equal(1, delivery.Attempt);
        }

        // Regresie pentru abonamentele durabile: publicat cu receiverul deconectat
        [Fact]
        public async Task Publish_WhileReceiverOffline_IsDeliveredAfterResubscribe()
        {
            await using var broker = await BrokerServerFixture.StartAsync();
            SubscribeRequest request = NewSubscribe("bob", "test");

            // Prima conectare doar ca sa persiste abonamentul, apoi se inchide
            using (var firstCts = new CancellationTokenSource())
            {
                using var first = broker.Client.Subscribe(request, cancellationToken: firstCts.Token);
                await WaitForSubscriptionAsync(broker, "test");
                firstCts.Cancel();
            }

            await broker.Client.PublishAsync(NewPublish("test", "while offline"));

            using var cts = new CancellationTokenSource(Timeout);
            using var second = broker.Client.Subscribe(request, cancellationToken: cts.Token);

            Assert.True(await second.ResponseStream.MoveNext(cts.Token));
            Assert.Equal("while offline", second.ResponseStream.Current.Message.Data.Content);
        }

        [Fact]
        public async Task Acknowledge_IsIdempotent()
        {
            await using var broker = await BrokerServerFixture.StartAsync();
            using var cts = new CancellationTokenSource(Timeout);

            using var call = broker.Client.Subscribe(NewSubscribe("alice", "test"), cancellationToken: cts.Token);
            await WaitForSubscriptionAsync(broker, "test");
            await broker.Client.PublishAsync(NewPublish("test", "hello"));
            Assert.True(await call.ResponseStream.MoveNext(cts.Token));

            var ack = new AckRequest
            {
                ReceiverId = ReceiverIdentity.FromName("alice").ToString(),
                MessageAcknowledged = call.ResponseStream.Current.Message.MessageId
            };

            AckReply first = await broker.Client.AcknowledgeAsync(ack);
            AckReply second = await broker.Client.AcknowledgeAsync(ack);

            Assert.True(first.Recorded);
            // Deja Acked, al doilea ACK nu mai modifica niciun rand
            Assert.False(second.Recorded);
        }
    }
}