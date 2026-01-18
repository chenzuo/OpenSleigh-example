using Microsoft.AspNetCore.Mvc;
using OpenSleigh.Transport;
using WebApplication7.Domain.Messages;
using WebApplication7.Domain.Sagas;
using WebApplication7.Domain.States;
using WebApplication7.Infrastructure;

namespace WebApplication7.Controllers
{
    [ApiController]
    [Route("api/refunds")]
    public class RefundController : ControllerBase
    {
        private static readonly IReadOnlyDictionary<string, string> StepAliases = new Dictionary<
            string,
            string
        >(StringComparer.OrdinalIgnoreCase)
        {
            ["RefundRequested"] = "Reserve",
            ["ReserveRefund"] = "Reserve",
            ["RefundReserved"] = "Reserved",
            ["ExecuteRefund"] = "Execute",
        };

        private static readonly IReadOnlyDictionary<
            string,
            Func<Guid, string, IMessage>
        > StepPublishers = new Dictionary<string, Func<Guid, string, IMessage>>(
            StringComparer.OrdinalIgnoreCase
        )
        {
            ["Reserve"] = (orderId, correlationId) => new ReserveRefund(orderId, correlationId),
            ["Reserved"] = (orderId, correlationId) => new RefundReserved(orderId, correlationId),
            ["Execute"] = (orderId, correlationId) => new ExecuteRefund(orderId, correlationId),
        };

        private readonly IMessageBus _bus;
        private readonly ISagaStateReader _stateReader;
        private readonly ILogger<RefundController> _logger;

        public RefundController(
            IMessageBus bus,
            ISagaStateReader stateReader,
            ILogger<RefundController> logger
        )
        {
            _bus = bus ?? throw new ArgumentNullException(nameof(bus));
            _stateReader = stateReader ?? throw new ArgumentNullException(nameof(stateReader));
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

            LogInformation("refund requested for order {OrderId}", orderId);

            return Results.Accepted(
                $"/api/refunds/{correlationId}",
                new RefundAcceptedResponse(
                    orderId,
                    correlationId,
                    dto.Amount,
                    dto.Reason,
                    false,
                    "refund-request-accepted"
                )
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
                return ValidationError(dto.CorrelationId, "correlationId is required");

            var sagaView = await GetSagaStateAsync(correlationId, cancellationToken);
            var state = sagaView?.State;
            if (state is null)
                return ValidationError(correlationId, "saga state not found");

            if (string.IsNullOrWhiteSpace(step))
            {
                var resolvedStep = state.LastFailedStep;
                if (string.IsNullOrWhiteSpace(resolvedStep))
                    return ValidationError(
                        correlationId,
                        "step is required or last failed step not found"
                    );
                step = resolvedStep;
                resolvedFromState = true;
            }

            step = NormalizeStep(step);

            if (!await PublishStepMessage(state.OrderId, correlationId, step, cancellationToken))
                return ValidationError(correlationId, "step must be Reserve, Reserved, or Execute");

            LogInformation(
                "refund retry requested: CorrelationId={CorrelationId}, Step={Step}",
                correlationId,
                step
            );

            return Results.Accepted(
                $"/api/refunds/{correlationId}",
                new RefundRetryResponse(correlationId, step, resolvedFromState)
            );
        }

        private async Task<SagaStateView<RefundSagaState>?> GetSagaStateAsync(
            string correlationId,
            CancellationToken cancellationToken
        )
        {
            var sagaTypeName = typeof(RefundSaga).FullName!;

            LogInformation(
                "Resolving saga state: CorrelationId={CorrelationId}, SagaType={SagaType}",
                correlationId,
                sagaTypeName
            );

            var stateView = await _stateReader.GetStateAsync<RefundSaga, RefundSagaState>(
                correlationId,
                cancellationToken
            );

            if (stateView is null)
            {
                _logger.LogWarning(
                    "No saga state found for CorrelationId={CorrelationId}",
                    correlationId
                );
                return null;
            }

            LogInformation(
                "Resolved saga state: Status={Status}, LastFailedStep={LastFailedStep}",
                stateView.State?.Status,
                stateView.State?.LastFailedStep
            );

            return stateView;
        }

        private async Task<bool> PublishStepMessage(
            Guid orderId,
            string correlationId,
            string step,
            CancellationToken cancellationToken
        )
        {
            LogInformation(
                "PublishStepMessage: CorrelationId={CorrelationId}, Step='{Step}' (Length={Length})",
                correlationId,
                step,
                step?.Length ?? 0
            );

            if (
                string.IsNullOrWhiteSpace(step)
                || !StepPublishers.TryGetValue(step, out var factory)
            )
            {
                _logger.LogWarning(
                    "Invalid step: '{Step}'. Valid steps are: Reserve, Reserved, Execute",
                    step
                );
                return false;
            }

            await _bus.PublishAsync(factory(orderId, correlationId), cancellationToken);
            return true;
        }

        private static string NormalizeStep(string step)
        {
            if (string.IsNullOrWhiteSpace(step))
                return string.Empty;

            var trimmed = step.Trim();
            return StepAliases.TryGetValue(trimmed, out var normalized) ? normalized : trimmed;
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
                return ValidationError(dto.CorrelationId, "correlationId is required");

            var sagaView = await GetSagaStateAsync(correlationId, cancellationToken);
            var state = sagaView?.State;
            if (state is null)
                return ValidationError(correlationId, "saga state not found");

            if (string.IsNullOrWhiteSpace(dto.Step))
                return ValidationError(correlationId, "step is required");

            await _bus.PublishAsync(
                new RefundCompensate(state.OrderId, correlationId, dto.Step),
                cancellationToken
            );

            LogInformation(
                "refund compensation requested: CorrelationId={CorrelationId}, Step={Step}",
                correlationId,
                dto.Step
            );

            return Results.Accepted(
                $"/api/refunds/{correlationId}",
                new RefundCompensateResponse(
                    correlationId,
                    dto.Step,
                    false,
                    "compensation accepted"
                )
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
                return ValidationError(correlationId, "correlationId is required");
            var sagaView = await _stateReader.GetStateAsync<RefundSaga, RefundSagaState>(
                correlationId,
                cancellationToken
            );

            if (sagaView is null)
                return NotFoundResponse(correlationId, "saga state not found");

            var summary = sagaView.State is null
                ? null
                : new RefundStateSummary(
                    sagaView.State.CorrelationId,
                    sagaView.State.Status,
                    sagaView.State.LastFailedStep,
                    sagaView.State.Amount,
                    sagaView.State.Reason,
                    sagaView.State.LastUpdated,
                    sagaView.State.OrderId
                );

            var response = new RefundStatusResponse(
                correlationId,
                sagaView.SagaInstanceId,
                sagaView.Completed,
                sagaView.Completed ? "completed" : "in-progress",
                summary,
                sagaView.ProcessedMessages
            );

            return Results.Ok(response);
        }

        private static IResult ValidationError(string? correlationId, string message) =>
            Results.BadRequest(new ApiError(correlationId, false, message));

        private static IResult NotFoundResponse(string correlationId, string message) =>
            Results.NotFound(new ApiError(correlationId, false, message, "not-found"));

        private void LogInformation(string message, params object?[] args)
        {
            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation(message, args);
        }
    }

    public record RefundRequestDto(decimal Amount, string Reason);

    public record RefundRetryRequestDto(string CorrelationId, string? Step);

    public record RefundCompensateRequestDto(string CorrelationId, string Step);

    public record RefundAcceptedResponse(
        Guid OrderId,
        string CorrelationId,
        decimal Amount,
        string Reason,
        bool Complate,
        string Message
    );

    public record RefundRetryResponse(string CorrelationId, string Step, bool ResolvedFromState);

    public record RefundCompensateResponse(
        string CorrelationId,
        string Step,
        bool Complate,
        string Message
    );

    public record RefundStatusResponse(
        string CorrelationId,
        string SagaInstanceId,
        bool Complate,
        string Message,
        RefundStateSummary? State,
        IReadOnlyCollection<ProcessedMessageView> ProcessedMessages
    );

    public record RefundStateSummary(
        string? CorrelationId,
        string Status,
        string? LastFailedStep,
        decimal Amount,
        string? Reason,
        DateTimeOffset LastUpdated,
        Guid OrderId
    );

    public record ApiError(
        string? CorrelationId,
        bool Complate,
        string Message,
        string? Status = null
    );
}
