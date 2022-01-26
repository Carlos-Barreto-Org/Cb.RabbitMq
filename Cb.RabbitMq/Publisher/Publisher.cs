namespace Cb.RabbitMq;

public class Publisher : IPublisher
{
    private readonly IModel _model;
    private readonly IConnection _connection;
    private readonly ILogger<Publisher> _logger;
    public Publisher(IModel model, ILogger<Publisher> logger, IConnection connection)
    {
        _model = model;
        _logger = logger;
        _connection = connection;
    }


    public bool Publish<TRequest>(string exchange, string routingKey, TRequest request)
        where TRequest : class
    {
        try
        {
            _model.ConfirmSelect(); // ack na publicação

            var propriedades = ObterPropriedades();

            _model.BasicPublish(exchange, routingKey, propriedades, request.ToRequestMessage());

            _model.WaitForConfirmsOrDie(TimeSpan.FromSeconds(15)); //Ack na publicação.

            //Dispose();

            return true;
        }
        catch (Exception ex)
        {
            Dispose();
            _logger.LogError($"Ocorreu um erro ao publicar a mensagem. Exception: {ex.Message}. StackTrace: {ex.StackTrace}");
            return false;
        }
    }

    private IBasicProperties ObterPropriedades()
    {
        var propriedades = _model.CreateBasicProperties();
        propriedades.Headers = new Dictionary<string, object>
        {
            { "content-type", "application/json" }
        };

        propriedades.DeliveryMode = 2; // sempre persistente
        propriedades.MessageId = Guid.NewGuid().ToString("D");
        return propriedades;
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

