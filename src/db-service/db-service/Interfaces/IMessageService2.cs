using db_service.Models;
using shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace db_service.Interfaces
{
    public interface IMessageService2
    {
        Task<bool> StoreMessageIfNewAsync(MessageEnvelope message, IReadOnlyCollection<Guid> receiverIds);
        Task EnsureCreatedAsync();
        Task<List<DueDelivery>> GetDueDeliveriesAsync(DateTime now, IReadOnlyCollection<Guid> connectedReceiverIds, int batchSize);
        Task<MessageEnvelope?> LoadEnvelopeAsync(Guid messageId);
        Task<int> RecordAttemptAsync(Guid messageId, Guid receiverId, DateTime nextAttemptAt);

        // IDEMPOTENT METHODS
        Task<int> MarkAckedAsync(Guid messageId, Guid receiverID);
        Task<int> MarkRetryLaterAsync(Guid messageId, Guid receiverId, string reason, DateTime nextAttemptAt);
        Task<int> MarkDeadLetterAsync(Guid messageId, Guid receiverId, string reason);

    }
}
