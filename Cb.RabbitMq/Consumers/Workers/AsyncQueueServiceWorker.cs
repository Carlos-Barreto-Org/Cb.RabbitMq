using RabbitMQ.Client.Events;

namespace Cb.RabbitMq.Consumers;

public class AsyncQueueServiceWorker<TRequest, TResponse> : QueueServiceWorkerBase
{

    private readonly Func<TRequest, Task> fireAndForgetDispatcher;
    private readonly Func<TRequest, Task<TResponse>> rpcDispatcher;

    private EQueueServiceWorkerMode Mode { get; }

    #region Constructors 


    public AsyncQueueServiceWorker(ILogger logger, IConnection connection,
    string queueName, ushort prefetchCount, Func<TRequest, Task<TResponse>> rpcDispatcher)
        : base(logger, connection, queueName, prefetchCount)
    {
        this.Mode = EQueueServiceWorkerMode.RPC;
        this.rpcDispatcher = rpcDispatcher;
    }

    public AsyncQueueServiceWorker(ILogger logger, IConnection connection, string queueName, ushort prefetchCount, Func<TRequest, Task> fireAndForgetDispatcher)
        : base(logger, connection, queueName, prefetchCount)
    {
        this.Mode = EQueueServiceWorkerMode.FireAndForget;
        this.fireAndForgetDispatcher = fireAndForgetDispatcher;
    }

    #endregion


    protected override IBasicConsumer BuildConsumer()
    {
        var consumer = new AsyncEventingBasicConsumer(this.Model);

        consumer.Received += this.Receive;

        return consumer;
    }

    private async Task Receive(object sender, BasicDeliverEventArgs ea)
    {
        TRequest request;
        try
        {
            request = ObterRequestMessage<TRequest>(ea);
        }
        catch (Exception exception)
        {
            this.Model.BasicReject(ea.DeliveryTag, false);

            this._logger.LogWarning("Message rejected during desserialization {exception}", exception);

            return;
        }


        try
        {
            if (this.Mode == EQueueServiceWorkerMode.FireAndForget)
            {
                await this.fireAndForgetDispatcher(request);
            }
            else if (this.Mode == EQueueServiceWorkerMode.RPC)
            {
                if (ea.BasicProperties.ReplyTo == null)
                {
                    this.Model.BasicReject(ea.DeliveryTag, false);

                    this._logger.LogWarning("Message rejected because doesn't sent a ReplyTo header to delivery feedback of RPC");

                    return;
                }

                TResponse responsePayload = await rpcDispatcher(request);

                IBasicProperties responseProperties = Model.CreateBasicProperties()
                                                                .SetMessageId()
                                                                .SetCorrelationId(ea.BasicProperties);

                this.Model.BasicPublish(string.Empty, ea.BasicProperties.ReplyTo, responseProperties,
                    responsePayload.ToRequestMessage());

            }

            this.Model.BasicAck(ea.DeliveryTag, false);
        }
        catch (Exception exception)
        {

            this.Model.BasicNack(ea.DeliveryTag, false, false);

            this._logger.LogWarning("Exception on processing message {message} {exception}", this.QueueName, exception);

        }
    }
}
