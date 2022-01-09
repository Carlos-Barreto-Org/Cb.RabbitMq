using RabbitMQ.Client.Events;

namespace Cb.RabbitMq.Consumers
{


    public class SyncQueueServiceWorker<TRequest, TResponse> : QueueServiceWorkerBase
    {
        private readonly Action<TRequest> fireAndForgetDispatcher;
        private readonly Func<TRequest, TResponse> rpcDispatcher;

        private EQueueServiceWorkerMode Mode { get; }

        #region Constructors 


        public SyncQueueServiceWorker(ILogger<SyncQueueServiceWorker<TRequest, TResponse>> logger, IConnection connection, string queueName, ushort prefetchCount, Func<TRequest, TResponse> rpcDispatcher)
            : base(logger, connection, queueName, prefetchCount)
        {            
            this.Mode = EQueueServiceWorkerMode.RPC;
            this.rpcDispatcher = rpcDispatcher;
        }

        public SyncQueueServiceWorker(ILogger<SyncQueueServiceWorker<TRequest, TResponse>> logger, IConnection connection, string queueName, ushort prefetchCount, Action<TRequest> fireAndForgetDispatcher)
            : base(logger, connection, queueName, prefetchCount)
        {
            this.Mode = EQueueServiceWorkerMode.FireAndForget;
            this.fireAndForgetDispatcher = fireAndForgetDispatcher;
        }

        #endregion


        protected override IBasicConsumer BuildConsumer()
        {
            var consumer = new EventingBasicConsumer(this.Model);

            consumer.Received += this.Receive;

            return consumer;
        }

        private void Receive(object sender, BasicDeliverEventArgs ea)
        {
            TRequest request;
            try
            {
                request = ObterRequestMessage<TRequest>(ea);
            }
            catch (Exception exception)
            {
                this.Model.BasicReject(ea.DeliveryTag, false);

                this._logger.LogWarning("Mensagem rejeitada durante e desserealização {exception}", exception);

                return;
            }


            try
            {
                if (this.Mode == EQueueServiceWorkerMode.FireAndForget)
                {
                    this.fireAndForgetDispatcher(request);
                }
                else if (this.Mode == EQueueServiceWorkerMode.RPC)
                {
                    if (ea.BasicProperties.ReplyTo == null)
                    {
                        this.Model.BasicReject(ea.DeliveryTag, false);

                        this._logger.LogWarning("Mensagem rejeita porque não foi enviado o cabeçalho ReplyTo para entragar o retorno do RPC");

                        return;
                    }

                    TResponse response = this.rpcDispatcher(request);

                    IBasicProperties responseProperties = this.Model.CreateBasicProperties()
                                                                    .SetMessageId()
                                                                    .SetCorrelationId(ea.BasicProperties);

                    this.Model.BasicPublish(string.Empty, ea.BasicProperties.ReplyTo, responseProperties, response.ToRequestMessage());

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
}
