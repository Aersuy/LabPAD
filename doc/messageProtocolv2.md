## Partea 2 - gRPC

Arhitectura e aceeasi ca in partea 1 (sender -> broker -> receiver, subiecte, storage persistent SQLite, ack/nack, retry, dead letter),
doar comunicarea e facuta cu gRPC in loc de socketuri + `MessageProtocol`.

Contractul e definit in `src/shared/grpc-contracts/Protos/broker.proto` (proto3), din el se genereaza clientul si serverul.

Transport: HTTP/2 peste TCP (gRPC nu suporta UDP), plaintext, port default 6002.

## Serviciul BrokerService

`Publish(PublishRequest) returns (PublishReply)` - unary, folosit de sender. Raspunsul e Ack-ul.

`Subscribe(SubscribeRequest) returns (stream Delivery)` - server stream, folosit de receiver (Publisher / Subscriber, push).
Stream-ul ramane deschis cat timp receiverul e conectat.

`Acknowledge(AckRequest) returns (AckReply)` - receiverul confirma un mesaj.

`Reject(NackRequest) returns (AckReply)` - Nack, `retryable` = true -> retry dupa 5s, false -> dead letter.

## Envelope

Name,Type,Required

message_id,`string (guid)`,yes

sender_id,`string (guid)`,yes

time_stamp,`google.protobuf.Timestamp`,yes   // Note, UTC

subject,`repeated string`,yes (minim 1, fara subiecte goale)

version,`int32`,yes (>= 1)

payload,`oneof { DataPayload data }`,yes

`DataPayload`
{ content: string }  // < 1,000,000 bytes, aceeasi limita ca MessageProtocol.MaxBytes

## Validare

`EnvelopeMapper.ToMessageEnvelope` valideaza fiecare mesaj primit si il transforma in `shared.Models.MessageEnvelope`
(acelasi model folosit de db-service si de `DataHandler1`). Mesaj invalid -> `InvalidDataException`.

Broker: mesaj invalid -> `StatusCode.InvalidArgument`, senderul nu mai reincearca.
Eroare la storage -> `StatusCode.Unavailable`, senderul reincearca cu acelasi MessageId (brokerul ignora duplicatele).

Receiver: mesaj invalid sau versiune necunoscuta -> Nack fara retry.

## Politici de livrare

Sender: maxim 5 incercari, deadline 5s pe fiecare Publish.

Broker: maxim 5 incercari per receiver, ack timeout 5s, dupa care mesajul e dead letter.

Caderea brokerului: mesajele si livrarile sunt in SQLite, receiverul se reconecteaza la 3s cu acelasi id
si primeste livrarile ramase. `EffectStore` garanteaza ca un mesaj e aplicat o singura data.

## Concurenta

Fiecare apel gRPC e tratat concurent de ASP.NET Core (HTTP/2 multiplexing), fiecare receiver are propriul loop de trimitere.
Receiverii conectati sunt tinuti intr-un `ConcurrentDictionary` (`ReceiverRegistry`).
