using OpenSleigh;
using OpenSleigh.Transport;
using WebApplication7.Domain.Messages;
using WebApplication7.Domain.States;

namespace WebApplication7.Domain.Sagas
{
    public class RefundSaga
        : Saga<RefundSagaState>,
            IStartedBy<RefundRequested>,
            IHandleMessage<ReserveRefund>,
            IHandleMessage<RefundReserved>,
            IHandleMessage<ExecuteRefund>,
            IHandleMessage<RefundExecuted>,
            IHandleMessage<RefundCompensate>
    {
        private readonly ILogger<RefundSaga> _logger;
        private const int FailureRateFirstStep = 60;

        public RefundSaga(ILogger<RefundSaga> logger, ISagaInstance<RefundSagaState> context)
            : base(context)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async ValueTask HandleAsync(
            IMessageContext<RefundRequested> ctx,
            CancellationToken cancellationToken = default
        )
        {
            Context.State.CorrelationId = ctx.Message.CorrelationId;
            Context.State.OrderId = ctx.Message.OrderId;
            Context.State.Amount = ctx.Message.Amount;
            Context.State.Reason = ctx.Message.Reason;
            Context.State.Status = "Requested";
            Context.State.LastUpdated = DateTimeOffset.UtcNow;

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "退款流程开始: OrderId={OrderId}, Amount={Amount}, Reason={Reason}",
                    ctx.Message.OrderId,
                    ctx.Message.Amount,
                    ctx.Message.Reason
                );

            await WaitRandomDelayAsync(cancellationToken);

            if (ShouldFail("RefundRequested", ctx.Message.OrderId))
            {
                await FailAndStop("Reserve", ctx.Message.OrderId);
                return;
            }

            Publish(new ReserveRefund(ctx.Message.OrderId, ctx.Message.CorrelationId));
            return;
        }

        public async ValueTask HandleAsync(
            IMessageContext<ReserveRefund> ctx,
            CancellationToken cancellationToken = default
        )
        {
            Context.State.Status = "ReserveRequested";
            Context.State.LastUpdated = DateTimeOffset.UtcNow;

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("退款预留资金: OrderId={OrderId}", ctx.Message.OrderId);

            await WaitRandomDelayAsync(cancellationToken);

            if (ShouldFail("ReserveRefund", ctx.Message.OrderId))
            {
                await FailAndStop("Reserve", ctx.Message.OrderId);
                return;
            }

            Publish(new RefundReserved(ctx.Message.OrderId, ctx.Message.CorrelationId));
            return;
        }

        public async ValueTask HandleAsync(
            IMessageContext<RefundReserved> ctx,
            CancellationToken cancellationToken = default
        )
        {
            Context.State.Status = "Reserved";
            Context.State.LastUpdated = DateTimeOffset.UtcNow;

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("退款资金已预留: OrderId={OrderId}", ctx.Message.OrderId);

            await WaitRandomDelayAsync(cancellationToken);

            if (ShouldFail("RefundReserved", ctx.Message.OrderId))
            {
                await FailAndStop("Reserved", ctx.Message.OrderId);
                return;
            }

            Publish(new ExecuteRefund(ctx.Message.OrderId, ctx.Message.CorrelationId));
            return;
        }

        public async ValueTask HandleAsync(
            IMessageContext<ExecuteRefund> ctx,
            CancellationToken cancellationToken = default
        )
        {
            Context.State.Status = "Executing";
            Context.State.LastUpdated = DateTimeOffset.UtcNow;

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("开始执行退款: OrderId={OrderId}", ctx.Message.OrderId);

            await WaitRandomDelayAsync(cancellationToken);

            if (ShouldFail("ExecuteRefund", ctx.Message.OrderId))
            {
                await FailAndStop("Execute", ctx.Message.OrderId);
                return;
            }

            Publish(new RefundExecuted(ctx.Message.OrderId, ctx.Message.CorrelationId));
            return;
        }

        public async ValueTask HandleAsync(
            IMessageContext<RefundExecuted> ctx,
            CancellationToken cancellationToken = default
        )
        {
            Context.State.Status = "Completed";
            Context.State.LastUpdated = DateTimeOffset.UtcNow;

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("退款完成: OrderId={OrderId}", ctx.Message.OrderId);

            await WaitRandomDelayAsync(cancellationToken);
            Context.MarkAsCompleted();
            return;
        }

        public async ValueTask HandleAsync(
            IMessageContext<RefundCompensate> ctx,
            CancellationToken cancellationToken = default
        )
        {
            Context.State.Status = "Compensated";
            Context.State.LastUpdated = DateTimeOffset.UtcNow;

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "手动补偿执行: OrderId={OrderId}, Step={Step}",
                    ctx.Message.OrderId,
                    ctx.Message.Step
                );

            await WaitRandomDelayAsync(cancellationToken);
            return;
        }

        private ValueTask FailAndStop(string step, Guid orderId)
        {
            Context.State.Status = "Failed";
            Context.State.LastFailedStep = step;
            Context.State.LastUpdated = DateTimeOffset.UtcNow;

            _logger.LogWarning("退款流程随机失败: OrderId={OrderId}, Step={Step}", orderId, step);

            return ValueTask.CompletedTask;
        }

        private static bool ShouldFail(string step, Guid orderId)
        {
            _ = step;
            _ = orderId;
            var failureRatePercent = step switch
            {
                "RefundRequested" => FailureRateFirstStep,
                "ReserveRefund" => 45,
                "RefundReserved" => 30,
                "ExecuteRefund" => 15,
                _ => 1,
            };

            return Random.Shared.Next(0, 100) < failureRatePercent;
        }

        private static Task WaitRandomDelayAsync(CancellationToken cancellationToken)
        {
            var delayMilliseconds = Random.Shared.Next(1000, 5001);
            return Task.Delay(delayMilliseconds, cancellationToken);
        }
    }
}
