namespace Cb.RabbitMq;

public interface IMessageBus
{
    void Consume<TRequest>(string queue, Func<TRequest, Task> func)
        where TRequest : class;

    void ConsumeAndReply<TRequest, TResponse>(string queue, Func<TRequest, TResponse> func)
         where TRequest : class
        where TResponse : class;
     
    void ConsumeAndReplyAsync<TRequest, TResponse>(string queue, Func<TRequest, Task<TResponse>> func);         

    bool Publish<TRequest>(string exchange, string routingKey, PublishMessage<TRequest> request)
        where TRequest : class;

    Task<TResponse> SendAndReceiveAsync<TRequest, TResponse>(string exchangeName, string routingKey, PublishMessage<TRequest> requestModel, TimeSpan? timeOut = null);
}
