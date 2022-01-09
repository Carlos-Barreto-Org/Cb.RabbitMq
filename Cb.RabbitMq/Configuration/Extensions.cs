using Cb.RabbitMq.Consumers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

namespace Cb.RabbitMq;

public static class Extensions
{
    public static void CreateRetryAndUnroutedSchema(this IServiceCollection services, string appName, Dictionary<string, string> routes)
    {
        using var scope = services.BuildServiceProvider().CreateScope();

        var model = scope.ServiceProvider.GetService<IModel>();
        //Unrouted
        //Conjunto de Fila + exchange com o conteúdo que não foi roteado
        model.ExchangeDeclare($"{appName}_unrouted_exchange", "fanout", true, false, null);
        model.QueueDeclare($"{appName}_unrouted_queue", true, false, false, null);
        model.QueueBind($"{appName}_unrouted_queue", $"{appName}_unrouted_exchange", string.Empty, null);

        //Deadletter
        //Fila e exchange que acusam problemas irrecuperáveis que precisam de atenção maunal
        model.ExchangeDeclare($"{appName}_deadletter_exchange", "fanout", true, false, null);
        model.QueueDeclare($"{appName}_deadletter_queue", true, false, false, null);
        model.QueueBind($"{appName}_deadletter_queue", $"{appName}_deadletter_exchange", string.Empty, null);

        //Retry
        //Exchanges e Filas que possuem a demanda de suportar retry
        model.ExchangeDeclare($"{appName}_retry_exchange", "fanout", true, false, null);
        model.QueueDeclare($"{appName}_retry_queue", true, false, false, new Dictionary<string, object>() {
                { "x-dead-letter-exchange", $"{appName}_deadletter_exchange" },
                { "x-dead-letter-routing-key", "" }
            });
        model.QueueBind($"{appName}_retry_queue", $"{appName}_retry_exchange", string.Empty, null);

        model.ExchangeDeclare($"{appName}_service", "topic", true, false, new Dictionary<string, object>() {
                 { "alternate-exchange", $"{appName}_unrouted_exchange" }
            });
        foreach (var item in routes)
        {
            string routingKey = item.Key;
            string functionalName = item.Value;
            //EntryPoint
            //Ponto de entrada de processamento

            model.QueueDeclare($"{appName}_{functionalName}_queue", true, false, false, new Dictionary<string, object>() {
                    { "x-dead-letter-exchange", $"{appName}_retry_exchange" }
                });
            model.QueueBind($"{appName}_{functionalName}_queue", $"{appName}_service", routingKey, null);
        }
    }

    private static void CreateEventBus(this IServiceCollection services, string hubName)
    {
        using var scope = services.BuildServiceProvider().CreateScope();

        var model = scope.ServiceProvider.GetService<IModel>();

        //Unrouted
        //Conjunto de Fila + exchange com o conteúdo que não foi roteado
        model.ExchangeDeclare($"{hubName}_unrouted-exchange", "fanout", true, false, null);
        model.QueueDeclare($"{hubName}_unrouted-queue", true, false, false, null);
        model.QueueBind($"{hubName}_unrouted-queue", $"{hubName}_unrouted-exchange", string.Empty, null);


        model.ExchangeDeclare(hubName, "topic", true, false, new Dictionary<string, object>() {
                { "alternate-exchange", $"{hubName}_unrouted-exchange" }
            });

    }

    private static void CreateBusConsumer(this IServiceCollection services, string hubName, string appName, string hubRoutingKey, Dictionary<string, string> routes)
    {
        using var scope = services.BuildServiceProvider().CreateScope();

        var model = scope.ServiceProvider.GetService<IModel>();

        string exchangeName = $"{hubName}_{appName}-exchange";
        model.ExchangeDeclare(exchangeName, "topic", true, false, null);

        foreach (var item in routes)
        {
            var routingKey = item.Key;
            var functionalName = item.Value;

            var queueName = $"{hubName}_{appName}_{functionalName}_queue";

            model.QueueDeclare(queueName, true, false, false, new Dictionary<string, object>()
            {
                //{ "x-dead-letter-exchange", $"{hubName}_{appName}_retry_exchange" }
            });

            model.QueueBind(queueName, exchangeName, routingKey, null);
        }

        model.ExchangeBind(exchangeName, hubName, hubRoutingKey, null);

    }

    public static IBasicProperties SetMessageId(this IBasicProperties basicProperties, string messageId = null)
    {
        basicProperties.MessageId = messageId ?? Guid.NewGuid().ToString("D");
        return basicProperties;
    }

    public static IBasicProperties SetCorrelationId(this IBasicProperties basicProperties, IBasicProperties originalBasicProperties)
    {
        basicProperties.CorrelationId = originalBasicProperties.MessageId;
        return basicProperties;
    }

    public static IBasicProperties SetCorrelationId(this IBasicProperties basicProperties, string correlationId)
    {
        basicProperties.CorrelationId = correlationId;
        return basicProperties;
    }

    public static IBasicProperties SetReplyTo(this IBasicProperties basicProperties, string replyTo = null)
    {
        if (!string.IsNullOrEmpty(replyTo))
            basicProperties.ReplyTo = replyTo;
        return basicProperties;
    }

    public static IBasicProperties SetAppName(this IBasicProperties basicProperties, string replyTo = null)
    {
        if (!string.IsNullOrEmpty(replyTo))
            basicProperties.ReplyTo = replyTo;
        return basicProperties;
    }
    public static IBasicProperties GetReplyProps(BasicDeliverEventArgs ea, IModel model)
    {
        var replyProps = model.CreateBasicProperties();
        replyProps.ReplyTo = ea.BasicProperties.ReplyTo;
        replyProps.CorrelationId = ea.BasicProperties.CorrelationId;
        return replyProps;
    }

    public static ReadOnlyMemory<byte> ToRequestMessage(this object request)
    {
        var serialized = JsonSerializer.Serialize(request);
        var bytes = Encoding.UTF8.GetBytes(serialized);
        return new ReadOnlyMemory<byte>(bytes);
    }

    public static IServiceCollection AddRabbitMqConfiguration(this IServiceCollection services, IConfiguration configuration, string RabbitSectionKeyMqConfiguration)
    {
        services.AddSingleton(sp =>
        {
            var parametros = sp.GetRequiredService<IOptions<RegisterRabbitMQDependenciesParameters>>().Value;

            return new ConnectionFactory()
            {
                UserName = parametros.UserName,
                Password = parametros.Password,
                Port = parametros.Port,
                HostName = parametros.HostName,
                VirtualHost = parametros.VirtualHost,
                DispatchConsumersAsync = parametros.DispatchConsumersAsync,
            };
        });

        services.AddTransientWithRetry<IConnection, BrokerUnreachableException>(sp => sp.GetRequiredService<ConnectionFactory>().CreateConnection());
        services.AddTransient(sp => sp.GetRequiredService<IConnection>().CreateModel());

        services.AddSingleton<IConsumer, Consumer>();
        services.AddSingleton<IPublisher, Publisher>();
        services.AddSingleton<ISender, Sender>();
        services.AddSingleton<IMessageBus, MessageBus>();

        services.AddTransient<IRabbitMqConfiguration, RabbitMqConfiguration>();

        services.Configure<RegisterRabbitMQDependenciesParameters>(configuration.GetSection(RabbitSectionKeyMqConfiguration));

        return services;
    }

    public static IServiceCollection AddTransientWithRetry<TService, TKnowException>(this IServiceCollection services, Func<IServiceProvider, TService> implementationFactory)
        where TKnowException : Exception
        where TService : class
    {
        return services.AddTransient(sp =>
        {
            TService? returnValue = default;

            RetryPolicy policy = BuildPolicy<TKnowException>();

            policy.Execute(() =>
            {
                returnValue = implementationFactory(sp);
            });

            return returnValue;
        });
    }

    private static RetryPolicy BuildPolicy<TKnowException>(int retryCount = 5)
        where TKnowException : Exception
    {
        return Policy.Handle<TKnowException>()
                     .WaitAndRetry(retryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
    }

    public static TRequest? ObterRequestMessage<TRequest>(IModel model, BasicDeliverEventArgs ea)
    {
        TRequest? request;
        var body = ea.Body;
        var message = Encoding.UTF8.GetString(body.ToArray());
        request = Activator.CreateInstance<TRequest>();
        try
        {
            request = JsonSerializer.Deserialize<TRequest>(message);
        }
        catch (Exception)
        {
            model.BasicReject(ea.DeliveryTag, false);
            return default(TRequest);
        }

        return request;
    }

}