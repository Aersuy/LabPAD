using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Broker.Models
{
    public class SubjectDbModel
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<MessageDbModel> Messages { get; set; } = new();
    }
}
