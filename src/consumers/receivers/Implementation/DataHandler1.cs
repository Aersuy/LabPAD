using receivers.Interfaces;
using shared.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace receivers.Implementation
{
    public class DataHandler1 : IDataHandler
    {
        public int version => 1;

        public string Handle(MessageEnvelope message)
        {
            DataPayload? payload;
            try { payload = message.JsonPayload.Deserialize<DataPayload>(); }
            catch (JsonException ex) { throw new PermanentFailureException("Payload not right",ex); }

            if (payload is null)
            {
                throw new PermanentFailureException("Empty payload");
            }
            if (payload.Content is null)
                throw new PermanentFailureException("Content is null");
            return payload.Content;
        }
    }
}
