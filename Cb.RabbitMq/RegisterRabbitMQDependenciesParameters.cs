namespace Cb.RabbitMq;

public class RegisterRabbitMQDependenciesParameters
{
    public const string RabbitMqConfiguration = "RabbitMqConfiguration";

    public string UserName { get; set; }
    public string Password { get; set; }
    public int Port { get; set; }
    public string HostName { get; set; }
    public string VirtualHost { get; set; }
    public bool DispatchConsumersAsync { get; set; }
}
