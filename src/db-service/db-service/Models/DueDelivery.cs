using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace db_service.Models
{
    public record DueDelivery(Guid MessageId, Guid ReceiverId, int Attempts);
}
