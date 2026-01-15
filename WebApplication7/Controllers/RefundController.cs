using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using OpenSleigh.Persistence.SQL;
using OpenSleigh.Transport;
using OpenSleigh.Utils;
using WebApplication7.Domain.Messages;
using WebApplication7.Domain.Sagas;
using WebApplication7.Domain.States;

namespace WebApplication7.Controllers
{
    [ApiController]
    [Route("api/refunds")]
    public class RefundController : ControllerBase
    {
        private readonly IMessageBus _bus;
        private readonly SagaDbContext _dbContext;
        private readonly ISerializer _serializer;
        private readonly ILogger<RefundController> _logger;

        public RefundController(
            IMessageBus bus,
            SagaDbContext dbContext,
            ISerializer serializer,
            ILogger<RefundController> logger
        )
        {
            _bus = bus ?? throw new ArgumentNullException(nameof(bus));
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        [HttpPost]
        public async Task<IResult> RequestRefund(
            [FromBody] RefundRequestDto dto,
            CancellationToken cancellationToken
        )
        {
            var orderId = Guid.NewGuid();
            var correlationId = Guid.NewGuid().ToString("N");
            var message = new RefundRequested(orderId, correlationId, dto.Amount, dto.Reason);

            await _bus.PublishAsync(message, cancellationToken);

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("refund requested for order {OrderId}", orderId);

            return Results.Accepted(
                $"/api/refunds/{correlationId}",
                new
                {
                    orderId,
                    correlationId,
                    dto.Amount,
                    dto.Reason,
                    Complate = false,
                    message = "refund-request-accepted",
                }
            );
        }

        [HttpPost("retry")]
        public async Task<IResult> RetryRefundStep(
            [FromBody] RefundRetryRequestDto dto,
            CancellationToken cancellationToken
        )
        {
            var correlationId = NormalizeCorrelationId(dto.CorrelationId);
            var step = dto.Step;
            var resolvedFromState = false;

            if (string.IsNullOrWhiteSpace(correlationId))
                return Results.BadRequest(
                    new
                    {
                        correlationId = dto.CorrelationId,
                        Complate = false,
                        message = "correlationId is required",
                    }
                );

            var state = await GetSagaState(correlationId, cancellationToken);
            if (state is null)
                return Results.BadRequest(
                    new
                    {
                        correlationId,
                        Complate = false,
                        message = "saga state not found",
                    }
                );

            if (string.IsNullOrWhiteSpace(step))
            {
                var resolvedStep = state.LastFailedStep;
                if (string.IsNullOrWhiteSpace(resolvedStep))
                    return Results.BadRequest(
                        new
                        {
                            correlationId,
                            Complate = false,
                            message = "step is required or last failed step not found",
                        }
                    );
                step = resolvedStep;
                resolvedFromState = true;
            }

            step = NormalizeStep(step);

            if (!await PublishStepMessage(state.OrderId, correlationId, step, cancellationToken))
                return Results.BadRequest(
                    new
                    {
                        correlationId,
                        Complate = false,
                        message = "step must be Reserve, Reserved, or Execute",
                    }
                );

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "refund retry requested: CorrelationId={CorrelationId}, Step={Step}",
                    correlationId,
                    step
                );

            return Results.Accepted(
                $"/api/refunds/{correlationId}",
                new
                {
                    correlationId,
                    step,
                    resolvedFromState,
                }
            );
        }

        private async Task<RefundSagaState?> GetSagaState(
            string correlationId,
            CancellationToken cancellationToken
        )
        {
            var sagaTypeName = typeof(RefundSaga).FullName!;

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "Resolving saga state: CorrelationId={CorrelationId}, SagaType={SagaType}",
                    correlationId,
                    sagaTypeName
                );

            var entity = await _dbContext
                .SagaStates.AsNoTracking()
                .FirstOrDefaultAsync(
                    e => e.CorrelationId == correlationId && e.SagaType == sagaTypeName,
                    cancellationToken
                );

            if (entity?.StateData is not { Length: > 0 })
            {
                _logger.LogWarning(
                    "No saga state found for CorrelationId={CorrelationId}",
                    correlationId
                );
                return null;
            }

            var state =
                _serializer.Deserialize(entity.StateData, typeof(RefundSagaState))
                as RefundSagaState;

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "Resolved saga state: Status={Status}, LastFailedStep={LastFailedStep}",
                    state?.Status,
                    state?.LastFailedStep
                );

            return state;
        }

        private async Task<bool> PublishStepMessage(
            Guid orderId,
            string correlationId,
            string step,
            CancellationToken cancellationToken
        )
        {
            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "PublishStepMessage: CorrelationId={CorrelationId}, Step='{Step}' (Length={Length})",
                    correlationId,
                    step,
                    step?.Length ?? 0
                );

            if (string.Equals(step, "Reserve", StringComparison.OrdinalIgnoreCase))
            {
                await _bus.PublishAsync(
                    new ReserveRefund(orderId, correlationId),
                    cancellationToken
                );
                return true;
            }

            if (string.Equals(step, "Execute", StringComparison.OrdinalIgnoreCase))
            {
                await _bus.PublishAsync(
                    new ExecuteRefund(orderId, correlationId),
                    cancellationToken
                );
                return true;
            }

            if (string.Equals(step, "Reserved", StringComparison.OrdinalIgnoreCase))
            {
                await _bus.PublishAsync(
                    new RefundReserved(orderId, correlationId),
                    cancellationToken
                );
                return true;
            }

            _logger.LogWarning(
                "Invalid step: '{Step}'. Valid steps are: Reserve, Reserved, Execute",
                step
            );
            return false;
        }

        private static string NormalizeStep(string step)
        {
            if (string.IsNullOrWhiteSpace(step))
                return step;

            var trimmed = step.Trim();

            if (string.Equals(trimmed, "RefundRequested", StringComparison.OrdinalIgnoreCase))
                return "Reserve";
            if (string.Equals(trimmed, "ReserveRefund", StringComparison.OrdinalIgnoreCase))
                return "Reserve";
            if (string.Equals(trimmed, "RefundReserved", StringComparison.OrdinalIgnoreCase))
                return "Reserved";
            if (string.Equals(trimmed, "ExecuteRefund", StringComparison.OrdinalIgnoreCase))
                return "Execute";

            return trimmed;
        }

        private static string NormalizeCorrelationId(string correlationId) =>
            string.IsNullOrWhiteSpace(correlationId)
                ? correlationId
                : correlationId.Trim().ToLowerInvariant();

        [HttpPost("compensate")]
        public async Task<IResult> CompensateRefund(
            [FromBody] RefundCompensateRequestDto dto,
            CancellationToken cancellationToken
        )
        {
            var correlationId = NormalizeCorrelationId(dto.CorrelationId);

            if (string.IsNullOrWhiteSpace(correlationId))
                return Results.BadRequest(
                    new
                    {
                        correlationId = dto.CorrelationId,
                        Complate = false,
                        message = "correlationId is required",
                    }
                );

            var state = await GetSagaState(correlationId, cancellationToken);
            if (state is null)
                return Results.BadRequest(
                    new
                    {
                        correlationId,
                        Complate = false,
                        message = "saga state not found",
                    }
                );

            if (string.IsNullOrWhiteSpace(dto.Step))
                return Results.BadRequest(
                    new
                    {
                        correlationId,
                        Complate = false,
                        message = "step is required",
                    }
                );

            await _bus.PublishAsync(
                new RefundCompensate(state.OrderId, correlationId, dto.Step),
                cancellationToken
            );

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(
                    "refund compensation requested: CorrelationId={CorrelationId}, Step={Step}",
                    correlationId,
                    dto.Step
                );

            return Results.Accepted(
                $"/api/refunds/{correlationId}",
                new
                {
                    correlationId,
                    step = dto.Step,
                    Complate = false,
                    message = "compensation accepted",
                }
            );
        }

        [HttpGet("{correlationId}")]
        public async Task<IResult> GetRefundStatus(
            string correlationId,
            CancellationToken cancellationToken
        )
        {
            correlationId = NormalizeCorrelationId(correlationId);
            if (string.IsNullOrWhiteSpace(correlationId))
                return Results.BadRequest(
                    new
                    {
                        correlationId,
                        Complate = false,
                        message = "correlationId is required",
                    }
                );
            var sagaTypeName = typeof(RefundSaga).FullName!;

            var entity = await _dbContext
                .SagaStates.AsNoTracking()
                .Include(e => e.ProcessedMessages)
                .FirstOrDefaultAsync(
                    e => e.CorrelationId == correlationId && e.SagaType == sagaTypeName,
                    cancellationToken
                );

            if (entity is null)
                return Results.NotFound(
                    new
                    {
                        correlationId,
                        status = "not-found",
                        Complate = false,
                        message = "saga state not found",
                    }
                );

            RefundSagaState? state = null;
            if (entity.StateData is { Length: > 0 } payload)
            {
                state =
                    _serializer.Deserialize(payload, typeof(RefundSagaState)) as RefundSagaState;
            }

            var response = new
            {
                correlationId,
                sagaInstanceId = entity.InstanceId,
                Complate = entity.IsCompleted,
                message = entity.IsCompleted ? "completed" : "in-progress",
                state = state is null
                    ? null
                    : new
                    {
                        state.CorrelationId,
                        state.Status,
                        state.LastFailedStep,
                        state.Amount,
                        state.Reason,
                        state.LastUpdated,
                        state.OrderId,
                    },
                processedMessages = entity
                    .ProcessedMessages.OrderBy(pm => pm.When)
                    .Select(pm => new { pm.MessageId, pm.When }),
            };

            return Results.Ok(response);
        }
    }

    public record RefundRequestDto(decimal Amount, string Reason);

    public record RefundRetryRequestDto(string CorrelationId, string? Step);

    public record RefundCompensateRequestDto(string CorrelationId, string Step);
}
