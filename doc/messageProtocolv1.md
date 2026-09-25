Every message is a length-prefixed JSON frame over the transport (`ITransport`):
Fiecare mesaj e transmis ca un frame folosind `ITransport`, lungimea e prefixul

Byte 0-3 Lungimea
Byte 4-N Content

Lungimea maximuma a contentului e 1,000,000, definita de MessageProtocol.MaxBytes
Minima e 0

Enum-urile sunt serializate nu transmise ca int

## MessageEnvelope
Name,Type,Required
MessageType,enum,yes
MessageId`,guid,yes
SenderId,guid,yes
Timestamp,datetime,yes   // Note, UTC
Subject,list<string>,yes
JsonPayload,object,yes
Version,int,yes       //  Note, default = 1

Payloads
`DataPayload`
{ "Content": "string" }

`RegisterPayload`
{ "IpAddress": "string", "Port": 0, "Role": "Sender" }

`AckPayload`
{ "MessageAcknowledged": "guid" }

`NackPayload`
{ "MessageNacked": "guid", "Reason": "string", "Retryable": true }


Versioning

MessageEnvelope.Version identifies the envelope/payload schema version (currently always 1). No V2 payloads exist yet. Convention going forward: a breaking change to any payload shape bumps Version and gets a new message-v2.md rather than mutating this doc in place.

Versionare
`MessageEnvelope.Verion` identifică versiunea 
1,2,...,N(int)
`IDataHandler` defineste metode
`DataHandler1` implementeaza versiunea 1
Un dictionar cu formatul
`Dictionary<int,IDataHandler>` contine toti handleri necesari.
Eg:
`!_dataHandlers.TryGetValue(message.Version, out IDataHandler? handler)`
`string content = handler.Handle(message);`

