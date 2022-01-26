namespace Cb.RabbitMq;

public interface ISender
{
    Task<TResponse> SendAndReceiveAsync<TRequest, TResponse>(string exchangeName, string routingKey, TRequest requestModel, TimeSpan? timeOut = null);
}

