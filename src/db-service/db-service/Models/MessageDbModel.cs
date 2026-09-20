using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace db_service.Models
{
    public class MessageDbModel
    {
        public Guid Id { get; set; }
        public Guid SenderId { get; set; }
        public DateTime TimeStamp { get; set; }
        public int Version { get; set; }
        public List<SubjectDbModel> Subjects { get; set; } = new List<SubjectDbModel>();
        public string JsonPayload { get; set; } = string.Empty;
    }
}
