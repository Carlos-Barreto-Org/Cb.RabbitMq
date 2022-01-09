namespace Cb.RabbitMq;
public class Exchange
{
    public Exchange(string nomeExchange, ETipoExchange tipoExchange)
    {
        NomeExchange = nomeExchange;
        TipoExchange = tipoExchange;
        Duravel = true;
        AutoDelete = false;
        CriarUnroutedDefault = false;
    }

    public string NomeExchange { get; private set; }
    public ETipoExchange TipoExchange { get; private set; }

    public Dictionary<string, object> Arguments { get; private set; } = new();

    public Exchange AdicionarAlternateExchange(string nomeExchange, AlternateExchangeQueue alternateQueue)
    {
        if (PossuiAlternateExchange())
            throw new ArgumentException("Já existe uma exchange alternativa para esta Exchange");

        Arguments.Add("alternate-exchange", nomeExchange);
        AlternateQueue.Add(alternateQueue);
        return this;
    }



    public bool PossuiAlternateExchange() => Arguments.Any(a => a.Key == "alternate-exchange");
    public string NomeAlternateExchange() => Arguments.FirstOrDefault(a => a.Key == "alternate-exchange").Value.ToString();

    /// <summary>
    /// Duravel: Padrão True
    /// </summary>
    public bool Duravel { get; private set; }
    /// <summary>
    /// AutoDelete: Padrão False
    /// </summary>
    public bool AutoDelete { get; private set; }


    public bool CriarUnroutedDefault { get; private set; }

    public string NomeExchangeUnroutedDefault => $"{NomeExchange}_unrouted";
    public string NomeQueueUnroutedDefault => $"{NomeExchange}_unrouted_queue";

    public Exchange PossuiUnroutedDefault()
    {
        CriarUnroutedDefault = true;
        return this;
    }

    public Exchange NaoDuravel()
    {
        Duravel = false;
        return this;
    }

    public Exchange AutoDeletavel()
    {
        AutoDelete = true;
        return this;
    }

    public List<Queue> Queues { get; private set; } = new();
    public List<AlternateExchangeQueue> AlternateQueue { get; private set; } = new();

    public Exchange AdicionarQueue(Queue queue)
    {
        Queues.Add(queue);
        return this;
    }

}

public class AlternateExchangeQueue : Queue
{
    public AlternateExchangeQueue(string nomeQueue) : base(nomeQueue, routingKey: "")
    {
    }
}

public class Queue
{
    public Queue(string nomeQueue, string routingKey)
    {
        NomeQueue = nomeQueue;
        RoutingKey = routingKey;
        Duravel = true;
        AutoDelete = false;
        Exclusiva = false;
    }

    public string NomeQueue { get; private set; }
    public string RoutingKey { get; private set; }

    /// <summary>
    /// Duravel: Padrão True
    /// </summary>
    public bool Duravel { get; private set; }
    /// <summary>
    /// AutoDelete: Padrão False
    /// </summary>
    public bool AutoDelete { get; private set; }
    /// <summary>
    /// Exclusiva: Padrão False
    /// </summary>
    public bool Exclusiva { get; private set; }

    public Queue NaoDuravel()
    {
        Duravel = false;
        return this;
    }

    public Queue AutoDeletavel()
    {
        AutoDelete = true;
        return this;
    }

    public Queue QueueExclusiva()
    {
        Exclusiva = true;
        return this;
    }

}


public enum ETipoExchange
{
    topic,
    fanout
}