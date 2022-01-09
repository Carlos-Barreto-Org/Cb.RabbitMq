namespace Cb.RabbitMq;
public class PublishMessage<T>    
{
    public string Message { get; set; }
    public Guid AggregateId { get; set; }
    public DateTime Time { get; set; }

    public PublishMessage()
    {

    }
    public PublishMessage(string message, T body)
    {
        Message = message;
        Body = body;
    }

    public PublishMessage(T body)
    {
        Message = string.Empty;
        Body = body;
    }

    public T Body { get; set; }
}

public class PublishMessage
{
    public PublishMessage()
    {

    }
    public PublishMessage(string message)
    {
        AggregateId = Guid.NewGuid();
        Time = DateTime.Now;
        Message = message;
    }
    public string Message { get; set; }
    public Guid AggregateId { get; set; }
    public DateTime Time { get; set; }
}

