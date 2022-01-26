namespace Cb.RabbitMq;

public interface IPublisher
{
    bool Publish<TRequest>(string exchange, string routingKey, TRequest request)
        where TRequest : class;
}

