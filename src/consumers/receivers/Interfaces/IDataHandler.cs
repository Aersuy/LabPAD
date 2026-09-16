using shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace receivers.Interfaces
{
    public interface IDataHandler
    {
        int version {  get; }
        void Handle(MessageEnvelope message);
    }
}
