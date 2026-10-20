using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace db_service.Models
{
    // a durable subscription stored in a database
    public class SubscriptionDbModel
    {
        public Guid ReceiverId { get; set; }
        public string Name { get; set; } = string.Empty;
        public DateTime UpdatedAt { get; set; }
        public List<SubscriptionSubjectDbModel> Subjects { get; set; } = new();
    }

    public class SubscriptionSubjectDbModel
    {
        public Guid ReceiverId { get; set; }
        public string Subject { get; set; } = string.Empty;
        public SubscriptionDbModel Subscription { get; set; } = null!;
    }
}