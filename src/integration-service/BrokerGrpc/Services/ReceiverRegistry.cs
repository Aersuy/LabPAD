using System.Collections.Concurrent;

namespace BrokerGrpc.Services
{
    // Thread safe registry of the receivers that currently have an open Subscribe stream
    public class ReceiverRegistry
    {
        private readonly ConcurrentDictionary<Guid, ReceiverSubscription> _receivers = new();

        // Adds the subscription and returns the previous one with the same id (if any)
        public ReceiverSubscription? Add(ReceiverSubscription subscription)
        {
            ReceiverSubscription? previous = null;
            _receivers.AddOrUpdate(subscription.Id, subscription, (_, existing) => { previous = existing; return subscription; });
            return previous;
        }

        // Remove the receiver only if the subscription matches, to avoid removing a new subscription with the same id
        // Can't just check if the subscription is the same because of atomicity, so we check and remove in one operation
        public void Remove(ReceiverSubscription subscription) =>
            _receivers.TryRemove(new KeyValuePair<Guid, ReceiverSubscription>(subscription.Id, subscription));

        public IReadOnlyCollection<Guid> GetReceiverIdsForSubjects(IEnumerable<string> subjects) =>
            _receivers.Values
            .Where(r => r.Subject.Intersect(subjects).Any())
            .Select(r => r.Id)
            .ToList();
    }

    public sealed class ReceiverSubscription : IDisposable
    {
        public Guid Id { get; }
        public List<string> Subject { get; }
        // Cancelled when the stream ends or when the same receiver subscribes again
        public CancellationTokenSource Cancellation { get; }

        public ReceiverSubscription(Guid id, List<string> subject, CancellationToken callToken)
        {
            Id = id;
            Subject = subject;
            Cancellation = CancellationTokenSource.CreateLinkedTokenSource(callToken);
        }

        public void Cancel()
        {
            try { Cancellation.Cancel(); }
            catch (ObjectDisposedException) { }
        }

        public void Dispose() => Cancellation.Dispose();
    }
}
