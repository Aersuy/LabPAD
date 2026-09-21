using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace db_service.Models
{
    public enum DeliveryStatus { Pending, Acked, DeadLettered }

    public class DeliveryDbModel
    {
        public Guid MessageId { get; set; }
        public Guid ReceiverId { get; set; }
        public DeliveryStatus Status { get; set; }
        public int Attempts { get; set; }
        public DateTime NextAttemptAt { get; set; }
        public string? LastError { get; set; }
        public MessageDbModel Message { get; set; } = null!;
    }
}
