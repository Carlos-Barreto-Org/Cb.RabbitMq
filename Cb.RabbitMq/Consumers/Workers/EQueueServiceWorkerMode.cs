using System;
using System.Collections.Generic;
using System.Text;

namespace Cb.RabbitMq.Consumers
{
    public enum EQueueServiceWorkerMode
    {
        None,
        RPC,
        FireAndForget
    }
}
