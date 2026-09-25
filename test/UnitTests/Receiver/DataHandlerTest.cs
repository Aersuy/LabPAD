using receivers.Implementation;
using shared.Models;
using System.Text.Json;

namespace UnitTests;


public class DataHandlerTest
{
    [Fact]
    public void Handle_ValidPayload_ReturnsContent()
    {
        var handler = new DataHandler1();
        var envelope = new MessageEnvelope
        {
            JsonPayload = JsonSerializer.SerializeToElement(new DataPayload { Content = "hello" })
        };
        var result = handler.Handle(envelope);
        Assert.Equal("hello", result);
    }

    [Fact]
    public void Handle_NullPayload_ThrowsPermanentFailureException()
    {
        var handler = new DataHandler1();
        var envelope = new MessageEnvelope
        {
            JsonPayload = JsonDocument.Parse("null").RootElement
        };

        Assert.Throws<PermanentFailureException>(() => handler.Handle(envelope));
    }
    [Fact]
    public void Handle_NullContent_ThrowsPermanentFailureException()
    {
        var handler = new DataHandler1();
        var envelope = new MessageEnvelope
        {
            JsonPayload = JsonDocument.Parse("""{"Content": null}""").RootElement
        };

        Assert.Throws<PermanentFailureException>(() => handler.Handle(envelope));
    }
    [Fact]
    public void Handle_MalformedJson_ThrowsPermanentFailureException()
    {
        var handler = new DataHandler1();
        var envelope = new MessageEnvelope
        {
            JsonPayload = JsonDocument.Parse("\"just a string\"").RootElement
        };

        Assert.Throws<PermanentFailureException>(() => handler.Handle(envelope));
    }
}
