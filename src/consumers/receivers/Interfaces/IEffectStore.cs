using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace receivers.Interfaces
{
    public interface IEffectStore
    {
        public bool ApplyOnce(Guid messageId, string content);
    }
}
