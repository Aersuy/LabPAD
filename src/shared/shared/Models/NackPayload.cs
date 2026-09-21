using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace shared.Models
{
    public class NackPayload
    {
        public Guid MessageNacked { get; set; }
        public string Reason { get; set; } = string.Empty;
        public bool Retryable { get; set; }
    }
}
