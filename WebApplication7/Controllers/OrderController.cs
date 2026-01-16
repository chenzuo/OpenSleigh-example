using Microsoft.AspNetCore.Mvc;
using OpenSleigh.Transport;
using WebApplication7.Domain.Messages;
using WebApplication7.Domain.Sagas;
using WebApplication7.Domain.States;
using WebApplication7.Infrastructure;

namespace WebApplication7.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class OrderController : ControllerBase
    {
        private readonly IMessageBus _bus;
        private readonly ISagaStateReader _stateReader;
        private readonly ILogger<OrderController> _logger;

        public OrderController(
            IMessageBus bus,
            ISagaStateReader stateReader,
            ILogger<OrderController> logger
        )
        {
            _bus = bus ?? throw new ArgumentNullException(nameof(bus));
            _stateReader = stateReader ?? throw new ArgumentNullException(nameof(stateReader));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        [HttpPost("saga/create")]
        public async Task<IResult> CreateOrder(CancellationToken cancellationToken)
        {
            var orderId = Guid.NewGuid();
            var message = new StartSaga(orderId);

            await _bus.PublishAsync(message, cancellationToken);

            if (_logger.IsEnabled(LogLevel.Information))
                _logger.LogInformation("order {OrderId} sent to saga", orderId);

            return Results.Accepted($"/api/order/{orderId}", new { orderId });
        }

        [HttpGet("saga/getstatus/{orderId:guid}")]
        public async Task<IResult> GetOrderStatus(Guid orderId, CancellationToken cancellationToken)
        {
            var correlationId = orderId.ToString("N");
            var sagaState = await _stateReader.GetStateAsync<SagaWithState, MySagaState>(
                correlationId,
                cancellationToken
            );

            if (sagaState is null)
                return Results.NotFound(new { orderId, status = "not-found" });

            var response = new OrderStatusResponse(
                orderId,
                sagaState.SagaInstanceId,
                sagaState.Completed,
                sagaState.State,
                sagaState.ProcessedMessages
            );

            return Results.Ok(response);
        }
    }

    public record OrderStatusResponse(
        Guid OrderId,
        string SagaInstanceId,
        bool Completed,
        MySagaState? State,
        IReadOnlyCollection<ProcessedMessageView> ProcessedMessages
    );
}
