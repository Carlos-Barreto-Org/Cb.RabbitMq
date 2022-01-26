using RabbitMQ.Client.Events;
using System.Collections.Concurrent;

namespace Cb.RabbitMq;
public class Sender : ISender
{
    private readonly IModel _model;
    private readonly IConnection _connection;
    private readonly ILogger<Sender> _logger;
    public Sender(IModel model, ILogger<Sender> logger, IConnection connection)
    {
        _model = model;
        _logger = logger;
        _connection = connection;
    }
    public async Task<TResponse> SendAndReceiveAsync<TRequest, TResponse>(string exchangeName, string routingKey, 
        TRequest requestModel, TimeSpan? timeOut = null)
    {
        timeOut ??= TimeSpan.FromMinutes(15);

        QueueDeclareOk queue = queue = _model.QueueDeclare(string.Empty, durable: false, exclusive: true, autoDelete: true, arguments: null);

        Task sendTask = Task.Run(() => { this.Send(exchangeName, routingKey, requestModel, queue.QueueName); });

        TResponse responseModel = default;

        using var localQueue = new BlockingCollection<TResponse>();

        Task receiveTask = Task.Run(() => { responseModel = Receive(queue, localQueue, timeOut); });

        await Task.WhenAll(sendTask, receiveTask);

        return responseModel;
    }

    private TResponse? Receive<TResponse>(QueueDeclareOk queue, BlockingCollection<TResponse> localQueue, TimeSpan? timeOut)
    {
        var consumer = new AsyncEventingBasicConsumer(_model);

        consumer.Received += (sender, ea) =>
        {
            var body = ea.Body;
            var message = Encoding.UTF8.GetString(body.ToArray());

            localQueue.Add(JsonSerializer.Deserialize<TResponse>(message));
            localQueue.CompleteAdding();

            return Task.CompletedTask;
        };

        var consumerTag = _model.BasicConsume(queue.QueueName, autoAck: true, consumer);

        TResponse responseModel;

        try
        {
            if (!localQueue.TryTake(out responseModel, timeOut.Value))
                throw new TimeoutException("Timeout na chamada AMQP RPC.");
        }
        finally
        {
            _model.BasicCancelNoWait(consumerTag);
        }

        return responseModel;
    }

    private void Send<TRequest>(string exchangeName, string routingKey, TRequest requestModel, string callbackQueueName)
    {
        var prop = _model.CreateBasicProperties();

        prop.MessageId = Guid.NewGuid().ToString("D");

        if (!string.IsNullOrEmpty(callbackQueueName))
            prop.ReplyTo = callbackQueueName;

        _model.BasicPublish(exchangeName, routingKey, prop, requestModel.ToRequestMessage());
    }

    public void Dispose()
    {
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
    }
}

