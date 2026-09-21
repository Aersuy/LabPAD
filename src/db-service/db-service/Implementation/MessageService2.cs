using db_service.Interfaces;
using db_service.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Internal;
using Microsoft.VisualBasic;
using shared.Enums;
using shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.Marshalling;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace db_service.Implementation
{
    public class MessageService2 : IMessageService2
    {
        private readonly IDbContextFactory<BrokerDbContext> _contextFactory;

        public MessageService2(IDbContextFactory<BrokerDbContext> contextFactory)
        {
            _contextFactory = contextFactory;
        }
        private async Task AddMessageAsync(BrokerDbContext db, MessageEnvelope message)
        {

            List<string> subjectNames = message.Subject.Distinct().ToList();

            List<SubjectDbModel> existingSubjects = await db.Subjects
                .Where(s => subjectNames.Contains(s.Name))
                .ToListAsync();
            List<string> existingNames = existingSubjects.Select(s => s.Name).ToList();

            List<SubjectDbModel> newSubjects = subjectNames
                .Except(existingNames)
                .Select(name => new SubjectDbModel { Name = name })
                .ToList();

            var storedMessage = new MessageDbModel
            {
                Id = message.MessageId,
                SenderId = message.SenderId,
                TimeStamp = message.TimeStamp,
                Version = message.Version,
                JsonPayload = message.JsonPayload.GetRawText(),
                Subjects = existingSubjects.Concat(newSubjects).ToList(),
            };

            db.Messages.Add(storedMessage);
        }
        private void AddDelivery(BrokerDbContext db, Guid messageID, IReadOnlyCollection<Guid> receiverIds)
        {
            var timeNow = DateTime.UtcNow;
            foreach (var receiver in receiverIds.Distinct())
            {
                var delivery = new DeliveryDbModel
                {
                    MessageId = messageID,
                    ReceiverId = receiver,
                    Status = DeliveryStatus.Pending,
                    Attempts = 0,
                    NextAttemptAt = timeNow,
                };
                db.Deliveries.Add(delivery);
            }
        }
        public async Task<bool> StoreMessageIfNewAsync(MessageEnvelope message, IReadOnlyCollection<Guid> receiverIds)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            await AddMessageAsync(db, message);
            AddDelivery(db, message.MessageId, receiverIds);
            try
            {
                await db.SaveChangesAsync();
                return true;
            }
            catch (DbUpdateException)
            {
                if (await MessageExistsAsync(message.MessageId))
                {
                    return false;
                }
                throw;
            }
        }
        /// <summary>
        /// IDEMPOTENT
        /// ATOMIC
        /// ONLY WORKS WHEN STATUS IS PENDING
        /// </summary>
        /// <param name="messageId"></param>
        /// <param name="receiverID"></param>
        /// <returns></returns>
        public async Task<int> MarkAckedAsync(Guid messageId, Guid receiverID)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            return await db.Deliveries.Where(d => d.MessageId == messageId && d.ReceiverId == receiverID && d.Status == DeliveryStatus.Pending)
                .ExecuteUpdateAsync(d => d.SetProperty(delivery => delivery.Status, DeliveryStatus.Acked)
                .SetProperty(d => d.LastError, string.Empty));
        }
        /// <summary>
        /// IDEMPOTENT
        /// ATOMIC
        /// ONLY WORKS WHEN STATUS IS PENDING
        /// </summary>
        /// <param name="messageId"></param>
        /// <param name="receiverId"></param>
        /// <param name="reason"></param>
        /// <param name="nextAttemptAt"></param>
        /// <returns></returns>
        public async Task<int> MarkRetryLaterAsync(Guid messageId, Guid receiverId, string reason, DateTime nextAttemptAt)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            return await db.Deliveries.Where(d => d.MessageId == messageId && d.ReceiverId == receiverId && d.Status == DeliveryStatus.Pending)
                .ExecuteUpdateAsync(d => d.SetProperty(delivery => delivery.LastError, reason)
                .SetProperty(delivery => delivery.NextAttemptAt, nextAttemptAt));
        }
        /// <summary>
        /// IDEMPOTENT
        /// ATOMIC
        /// ONLY WORKS WHEN STATUS IS PENDING
        /// </summary>
        /// <param name="messageId"></param>
        /// <param name="receiverId"></param>
        /// <param name="reason"></param>
        /// <returns></returns>
        public async Task<int> MarkDeadLetterAsync(Guid messageId, Guid receiverId, string reason)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            return await db.Deliveries.Where(d => d.MessageId == messageId && d.ReceiverId == receiverId && d.Status == DeliveryStatus.Pending)
                .ExecuteUpdateAsync(d => d.SetProperty(delivery => delivery.Status, DeliveryStatus.DeadLettered)
                .SetProperty(delivery => delivery.LastError, reason));
        }
        /// <summary>
        /// ATOMIC
        /// ONLY WORKS WHEN STATUS IS PENDING
        /// </summary>
        /// <param name="messageId"></param>
        /// <param name="receiverId"></param>
        /// <param name="nextAttemptAt"></param>
        /// <returns></returns>
        public async Task<int> RecordAttemptAsync(Guid messageId, Guid receiverId, DateTime nextAttemptAt)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            return await db.Deliveries.Where(d => d.MessageId == messageId && d.ReceiverId == receiverId && d.Status == DeliveryStatus.Pending)
                .ExecuteUpdateAsync(d => d.SetProperty(delivery => delivery.NextAttemptAt, nextAttemptAt)
                .SetProperty(delivery => delivery.Attempts, delivery => delivery.Attempts + 1));

        }
        public async Task<bool> MessageExistsAsync(Guid messageId)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            return await db.Messages.AnyAsync(m => m.Id == messageId);
        }
        public async Task<List<DueDelivery>> GetDueDeliveriesAsync(DateTime now, IReadOnlyCollection<Guid> connectedReceiverIds, int batchSize)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            return await db.Deliveries
                .Where(d => d.Status == DeliveryStatus.Pending && d.NextAttemptAt <= now && connectedReceiverIds.Contains(d.ReceiverId))
                .OrderBy(d => d.NextAttemptAt)
                .Take(batchSize)
                .Select(d => new DueDelivery(d.MessageId, d.ReceiverId, d.Attempts))
                .ToListAsync();
        }
        public async Task<MessageEnvelope?> LoadEnvelopeAsync(Guid messageId)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            var row = await db.Messages
              .Where(m => m.Id == messageId)
              .Select(m => new
              {
                  m.Id,
                  m.SenderId,
                  m.TimeStamp,
                  m.Version,
                  m.JsonPayload,
                  Subjects = m.Subjects.Select(s => s.Name).ToList()
              })
              .FirstOrDefaultAsync();
            if(row is null)
            {
                return null;
            }
            return new MessageEnvelope
            {
                MessageType = MessageType.Data,
                MessageId = row.Id,
                SenderId = row.SenderId,
                TimeStamp = row.TimeStamp,
                Version = row.Version,
                Subject = row.Subjects,
                JsonPayload = JsonSerializer.Deserialize<JsonElement>(row.JsonPayload)
            };
        }
        public async Task EnsureCreatedAsync()
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            await db.Database.EnsureCreatedAsync();
        }
    }
}
