using db_service.Interfaces;
using db_service.Models;
using Microsoft.EntityFrameworkCore;
using shared.Enums;
using shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace db_service.Implementation
{
    public class MessageService : IMessageService
    {
        private readonly IDbContextFactory<BrokerDbContext> _contextFactory;

        public MessageService(IDbContextFactory<BrokerDbContext> contextFactory)
        {
            _contextFactory = contextFactory;
        }

        public async Task EnsureCreatedAsync()
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            await db.Database.EnsureCreatedAsync();
        }

        public async Task StoreMessageAsync(MessageEnvelope message)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();

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
            await db.SaveChangesAsync();
        }

        public async Task<List<MessageEnvelope>> GetMessagesBySubjectsAsync(IEnumerable<string> subjects)
        {
            List<string> subjectNames = subjects.Distinct().ToList();

            await using var db = await _contextFactory.CreateDbContextAsync();

            List<MessageDbModel> stored = await db.Messages
                .Include(m => m.Subjects)
                .Where(m => m.Subjects.Any(s => subjectNames.Contains(s.Name)))
                .OrderBy(m => m.TimeStamp)
                .ToListAsync();

            return stored.Select(ToEnvelope).ToList();
        }
        public async Task<bool> MessageExistsAsync(Guid messageId)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            return await db.Messages.AnyAsync(m => m.Id == messageId);
        }

        // Store message if it is new return false if it already exists, otherwise return true
        // The reason to store the message first is that we make the db do the uniqueness check for us
        // If the message already exists, we get the DbUpdate exception and check if the message exists
        // this solves the issue of race conditions where 2 messages with the same id  are being stored at the same time
        // if we check whether the message exists first, the other message could be stored in between the check and the store
        // causing issues
        public async Task<bool> StoreMessageIfNewAsync(MessageEnvelope message)
        {
            try
            {
                await StoreMessageAsync(message);
                return true;                                   
            }
            catch (DbUpdateException)
            {
                if (await MessageExistsAsync(message.MessageId))
                    return false;                            
                throw;                                         
            }
        }
        private static MessageEnvelope ToEnvelope(MessageDbModel stored)
        {
            using JsonDocument doc = JsonDocument.Parse(stored.JsonPayload);

            return new MessageEnvelope
            {
                MessageType = MessageType.Data,
                MessageId = stored.Id,
                SenderId = stored.SenderId,
                TimeStamp = stored.TimeStamp,
                Version = stored.Version,
                Subject = stored.Subjects.Select(s => s.Name).ToList(),
                JsonPayload = doc.RootElement.Clone(),
            };
        }
    }
}

