using Microsoft.Extensions.Hosting;
using Polly;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Cb.RabbitMq.Consumers
{


    public abstract class QueueServiceWorkerBase : BackgroundService
    {
        protected readonly ILogger _logger;
        protected readonly IConnection connection;
        protected IModel Model { get; private set; }

        public ushort PrefetchCount { get; }
        public string QueueName { get; }

        #region Constructors 

        protected QueueServiceWorkerBase(ILogger logger, IConnection connection, string queueName, ushort prefetchCount)
        {
            this._logger = logger;
            this.connection = connection;
            this.QueueName = queueName;
            this.PrefetchCount = prefetchCount;
        }


        #endregion

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            this.Model = this.BuildModel();

            IBasicConsumer consumer = this.BuildConsumer();

            this.WaitQueueCreation();

            string consumerTag = consumer.Model.BasicConsume(
                             queue: this.QueueName,
                             autoAck: false,
                             consumer: consumer);

            while (!stoppingToken.IsCancellationRequested)
            {
                this._logger.LogInformation("Worker rodando: {time}", DateTimeOffset.Now);
                await Task.Delay(1000, stoppingToken);
            }

            this.Model.BasicCancelNoWait(consumerTag);
        }


        protected virtual void WaitQueueCreation()
        {
            Policy
            .Handle<OperationInterruptedException>()
                .WaitAndRetry(5, retryAttempt =>
                {
                    this._logger.LogWarning("Queue {QueueName} não encontrada... retentando", this.QueueName);
                    return TimeSpan.FromSeconds(Math.Pow(2, retryAttempt));
                })
                .Execute(() =>
                {
                    using (var testModel = this.BuildModel())
                    {
                        testModel.QueueDeclarePassive(this.QueueName);
                    }
                });
        }

        protected virtual IModel BuildModel()
        {
            IModel model = this.connection.CreateModel();

            model.BasicQos(0, this.PrefetchCount, false);

            return model;
        }

        protected abstract IBasicConsumer BuildConsumer();

        protected TRequest? ObterRequestMessage<TRequest>(BasicDeliverEventArgs ea)
        {
            return Extensions.ObterRequestMessage<TRequest>(Model, ea);
        }
    }
}
