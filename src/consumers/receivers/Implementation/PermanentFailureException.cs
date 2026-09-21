using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace receivers.Implementation
{
    // Permanent failure exception is thrown to mean no retries
    public class PermanentFailureException : Exception
    {
        public PermanentFailureException(string message, Exception? inner = null) : base(message, inner) { }
    }
}
