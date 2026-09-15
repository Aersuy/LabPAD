using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace shared.Models
{
    public class AckPayload
    {
        public Guid MessageAcknowledged { get; set; }
    }
}
