using Cb.RabbitMq.Consumers;

namespace Cb.RabbitMq;
public class MessageBus : IMessageBus
{
    private readonly IConsumer _consumer;
    private readonly IPublisher _publisher;
    private readonly ISender _sender;
    private readonly ILogger<MessageBus> _logger;
    public MessageBus(IConsumer consumer, IPublisher publisher, ILogger<MessageBus> logger, ISender sender)
    {
        _consumer = consumer;
        _publisher = publisher;
        _logger = logger;
        _sender = sender;
    }

    public void Consume<TRequest>(string queue, Func<TRequest, Task> func)
        where TRequest : class
    {
        _logger.LogInformation($"Consumindo fila {queue}. Hora: {DateTime.Now}");
        _consumer.ConsumeAsync(queue, func);
    }

    public void ConsumeAndReply<TRequest, TResponse>(string queue, Func<TRequest, TResponse> func)
        where TRequest : class
        where TResponse : class
    {
        _logger.LogInformation($"Consumindo fila {queue} por RPC. Hora: {DateTime.Now}");
        _consumer.ConsumeAndReplySync(queue, func);
    }

    public void ConsumeAndReplyAsync<TRequest, TResponse>(string queue, Func<TRequest, Task<TResponse>> func)
    {
        _logger.LogInformation($"Consumindo fila {queue} por RPC. Hora: {DateTime.Now}");
        _consumer.ConsumeAndReplyAsync(queue, func);
    }

    public bool Publish<TRequest>(string exchange, string routingKey, TRequest request)
        where TRequest : class
    {
        _logger.LogInformation($"Publicando. Exchange: {exchange}; Routing Key: {routingKey}; Hora: {DateTime.Now}");
        return _publisher.Publish(exchange, routingKey, request);
    }

    public Task<TResponse> SendAndReceiveAsync<TRequest, TResponse>(string exchangeName, string routingKey, TRequest requestModel, TimeSpan? timeOut = null) 
    {
        _logger.LogInformation($"Publicando RPC. Exchange: {exchangeName}; Routing Key: {routingKey}; Hora: {DateTime.Now}");
        return _sender.SendAndReceiveAsync<TRequest, TResponse>(exchangeName, routingKey, requestModel, timeOut);
    }
}
