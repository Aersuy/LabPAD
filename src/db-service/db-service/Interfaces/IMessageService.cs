using shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace db_service.Interfaces
{
    public interface IMessageService
    {
        Task EnsureCreatedAsync();
        Task StoreMessageAsync(MessageEnvelope message);
        Task<List<MessageEnvelope>> GetMessagesBySubjectsAsync(IEnumerable<string> subjects);
    }
}
