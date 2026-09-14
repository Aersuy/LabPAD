using shared.Enums;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.JavaScript;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;


namespace shared.Models
{
    public class MessageEnvelope
    {
        public MessageType MessageType { get; set; }
        public int MessageId { get; set; }
        public int SenderId { get; set; }
        public DateTime TimeStamp { get; set; }
        public List<string> Subject { get; set; } = new List<string>();
        public JsonElement JsonPayload { get; set; }
    }
}
