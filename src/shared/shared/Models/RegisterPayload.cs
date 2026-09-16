using shared.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace shared.Models
{
    public class RegisterPayload
    {
        public string IpAddress { get; set; } = string.Empty;
        public int Port { get; set; }
        public Roles Role { get; set; } //Sender or receiver

    }
}
