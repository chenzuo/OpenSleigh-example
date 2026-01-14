using OpenSleigh.Transport;

namespace WebApplication7.Domain.Messages
{
    public record StartSaga(Guid OrderId) : IMessage, IHasCorrelationId
    {
        public string CorrelationId => OrderId.ToString("N");
    }

    public record ProcessMySaga() : IMessage { }

    public record MySagaCompleted() : IMessage { }
}
