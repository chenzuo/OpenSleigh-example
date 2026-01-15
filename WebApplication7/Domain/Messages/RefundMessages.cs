using OpenSleigh.Transport;

namespace WebApplication7.Domain.Messages
{
    public record RefundRequested(Guid OrderId, string CorrelationId, decimal Amount, string Reason)
        : IMessage,
            IHasCorrelationId { }

    public record ReserveRefund(Guid OrderId, string CorrelationId)
        : IMessage,
            IHasCorrelationId { }

    public record RefundReserved(Guid OrderId, string CorrelationId)
        : IMessage,
            IHasCorrelationId { }

    public record ExecuteRefund(Guid OrderId, string CorrelationId)
        : IMessage,
            IHasCorrelationId { }

    public record RefundExecuted(Guid OrderId, string CorrelationId)
        : IMessage,
            IHasCorrelationId { }

    public record RefundCompensate(Guid OrderId, string CorrelationId, string Step)
        : IMessage,
            IHasCorrelationId { }
}
