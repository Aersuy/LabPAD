using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace shared.Enums
{
    public enum MessageType
    {
        Register,
        Disconnect,
        Data,
        Error,
        Ack,
        Nack,
    }
}
