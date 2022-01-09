using Microsoft.Extensions.Logging;
using RabbitMQ.Client.Events;

namespace Cb.RabbitMq.Consumers;

public interface IConsumer
{
    void ConsumeSync<TRequest>(string queue, Action<TRequest> func);
    void ConsumeAsync<TRequest>(string queue, Func<TRequest, Task> func);
    void ConsumeAndReplySync<TRequest, TResponse>(string queue, Func<TRequest, TResponse> func);
    void ConsumeAndReplyAsync<TRequest, TResponse>(string queue, Func<TRequest, Task<TResponse>> func);
}

public class Consumer : IConsumer
{
    private readonly IModel _model;
    private readonly IConnection _connection;
    private string _queue;
    private readonly uint PrefetchSize;
    private readonly ushort PrefetchCount;
    private readonly ILogger<Consumer> _logger;
    public string _consumerTag { get; private set; }
    public Consumer(IModel model, ILogger<Consumer> logger, IConnection connection)
    {
        _queue ??= "";
        _consumerTag ??= "";
        _model = model;
        PrefetchCount = 10;
        PrefetchSize = 0;
        _logger = logger;
        _connection = connection;
    }

    public void ConsumeSync<TRequest>(string queue, Action<TRequest> func) => Consume(queue, ConsumeSync(func));

    public void ConsumeAsync<TRequest>(string queue, Func<TRequest, Task> func) => Consume(queue, ConsumeAsync(func));

    public void ConsumeAndReplySync<TRequest, TResponse>(string queue, Func<TRequest, TResponse> func) => Consume(queue, ConsumeAndReply(func));

    public void ConsumeAndReplyAsync<TRequest, TResponse>(string queue, Func<TRequest, Task<TResponse>> func) => Consume(queue, ConsumeAndReplyAsync(func));

    private void Consume(string queue, AsyncEventHandler<BasicDeliverEventArgs> evento)
    {
        _queue = queue;

        _model.BasicQos(PrefetchSize, PrefetchCount, false);

        var consumer = new AsyncEventingBasicConsumer(_model);

        consumer.Received += evento;

        try
        {
            _consumerTag = _model.BasicConsume(_queue, false, consumer);
        }
        catch (Exception ex)
        {
            _logger.LogError($"Erro ao consumir serviço. ConsumerTag: {_consumerTag}. Exception: {ex.Message}. StackTrace: {ex.StackTrace}");
        }
    }

    private AsyncEventHandler<BasicDeliverEventArgs> ConsumeAsync<TRequest>(Func<TRequest, Task> func)
    {
        return async (sender, ea) =>
        {
            try
            {
                var request = Extensions.ObterRequestMessage<TRequest>(_model, ea);

                if (request == null) return;

                await func(request);

                _model.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception)
            {
                _model.BasicNack(ea.DeliveryTag, false, true);
            }
        };
    }

    private AsyncEventHandler<BasicDeliverEventArgs> ConsumeSync<TRequest>(Action<TRequest> func)
    {
        return async (sender, ea) =>
        {
            try
            {
                var request = Extensions.ObterRequestMessage<TRequest>(_model, ea);

                if (request == null) return;

                func(request);

                _model.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception)
            {
                _model.BasicNack(ea.DeliveryTag, false, true);
            }

        };
    }

    private AsyncEventHandler<BasicDeliverEventArgs> ConsumeAndReply<TRequest, TResponse>(Func<TRequest, TResponse> func)
    {
        return async (sender, ea) =>
        {
            try
            {
                var request = Extensions.ObterRequestMessage<TRequest>(_model, ea);

                if (request == null) return;

                var replyProps = Extensions.GetReplyProps(ea, _model);

                var response = func(request);

                _model.BasicPublish(exchange: "", routingKey: ea.BasicProperties.ReplyTo,
                     basicProperties: replyProps, body: response.ToRequestMessage());

                _model.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception)
            {
                _model.BasicNack(ea.DeliveryTag, false, true);
            }

        };
    }

    private AsyncEventHandler<BasicDeliverEventArgs> ConsumeAndReplyAsync<TRequest, TResponse>(Func<TRequest, Task<TResponse>> func)
    {
        return async (sender, ea) =>
        {
            try
            {
                var request = Extensions.ObterRequestMessage<TRequest>(_model, ea);

                if (request == null) return;

                var replyProps = Extensions.GetReplyProps(ea, _model);

                var response = await func(request);

                _model.BasicPublish(exchange: "", routingKey: ea.BasicProperties.ReplyTo,
                     basicProperties: replyProps, body: response.ToRequestMessage());

                _model.BasicAck(ea.DeliveryTag, false);
            }
            catch (Exception)
            {
                _model.BasicNack(ea.DeliveryTag, false, true);
            }

        };
    }
   


    public Consumer Stop()
    {
        if (!string.IsNullOrWhiteSpace(this._consumerTag))
        {
            _model.BasicCancel(this._consumerTag);
        }

        if (_model.IsOpen)
        {
            _model.Close();
            _model.Dispose();
        }

        if (_connection.IsOpen)
        {
            _connection.Close();
            _connection.Dispose();
        }

        return this;
    }

}
