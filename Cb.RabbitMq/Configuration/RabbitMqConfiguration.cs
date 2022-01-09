namespace Cb.RabbitMq;
public interface IRabbitMqConfiguration
{
    void AdicionarExchange(Exchange exchange);
    void Configure();
}


public class RabbitMqConfiguration : IRabbitMqConfiguration
{
    private readonly IModel _model;

    public RabbitMqConfiguration(IModel model)
    {
        _model = model;
    }

    private readonly List<Exchange> Exchanges = new List<Exchange>();

    public void AdicionarExchange(Exchange exchange)
    {
        Exchanges.Add(exchange);
    }

    public void Configure()
    {
        Exchanges.ForEach(exchange =>
        {
            if (exchange.CriarUnroutedDefault)
            {
                exchange.AdicionarAlternateExchange(
                            exchange.NomeExchangeUnroutedDefault,
                            new AlternateExchangeQueue(exchange.NomeQueueUnroutedDefault)
                );
            }

            DeclararExchange(exchange);

            if (exchange.PossuiAlternateExchange())
            {
                var alternateExchange = new Exchange(exchange.NomeAlternateExchange(), ETipoExchange.fanout);
                DeclararExchange(alternateExchange);
                exchange.AlternateQueue.ForEach(altQ => AdicionarQueue(alternateExchange, altQ));
            }

            exchange.Queues.ForEach(queue =>
            {
                AdicionarQueue(exchange, queue);
            });
        });
    }

    private void DeclararExchange(Exchange exchange)
    {
        _model.ExchangeDeclare(exchange.NomeExchange, exchange.TipoExchange.ToString(),
                exchange.Duravel, exchange.AutoDelete, exchange.Arguments);
    }

    private void AdicionarQueue(Exchange exchange, Queue queue)
    {
        _model.QueueDeclare(queue.NomeQueue, queue.Duravel, queue.Exclusiva, queue.AutoDelete);
        _model.QueueBind(queue.NomeQueue, exchange.NomeExchange, queue.RoutingKey);
    }
}
