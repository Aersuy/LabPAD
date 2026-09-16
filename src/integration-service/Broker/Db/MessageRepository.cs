using Broker.Models;
using Microsoft.EntityFrameworkCore;
using shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Broker.Db
{
    public interface IMessageRepository
    {
        Task EnsureCreatedAsync();
        Task StoreMessageAsync(MessageEnvelope msg);
    }

    public class MessageRepository : IMessageRepository
    {
        private readonly IDbContextFactory<BrokerDbContext> _contextFactory;

        public MessageRepository(IDbContextFactory<BrokerDbContext> contextFactory)
        {
            _contextFactory = contextFactory;
        }

        public async Task EnsureCreatedAsync()
        {
            await using var db = await _contextFactory.CreateDbContextAsync();
            await db.Database.EnsureCreatedAsync();
        }

        public async Task StoreMessageAsync(MessageEnvelope msg)
        {
            await using var db = await _contextFactory.CreateDbContextAsync();

            List<string> subjectNames = msg.Subject.Distinct().ToList();

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
                Id = msg.MessageId,
                SenderId = msg.SenderId,
                TimeStamp = msg.TimeStamp,
                Version = msg.Version,
                JsonPayload = msg.JsonPayload.GetRawText(),
                Subjects = existingSubjects.Concat(newSubjects).ToList(),
            };

            db.Messages.Add(storedMessage);
            await db.SaveChangesAsync();
        }
    }
}
