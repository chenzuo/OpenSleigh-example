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
    [Route("api/[controller]")]
    public class OrderController : ControllerBase
    {
        private readonly IMessageBus _bus;
        private readonly SagaDbContext _dbContext;
        private readonly ISerializer _serializer;
        private readonly ILogger<OrderController> _logger;

        public OrderController(
            IMessageBus bus,
            SagaDbContext dbContext,
            ISerializer serializer,
            ILogger<OrderController> logger
        )
        {
            _bus = bus ?? throw new ArgumentNullException(nameof(bus));
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        [HttpPost("saga/create")]
        public async Task<IResult> CreateOrder(CancellationToken cancellationToken)
        {
            var orderId = Guid.NewGuid();
            var message = new StartSaga(orderId);

            await _bus.PublishAsync(message, cancellationToken);
            _logger.LogInformation("order {OrderId} sent to saga", orderId);

            return Results.Accepted($"/api/order/{orderId}", new { orderId });
        }

        [HttpGet("saga/getstatus/{orderId:guid}")]
        public async Task<IResult> GetOrderStatus(Guid orderId, CancellationToken cancellationToken)
        {
            var correlationId = orderId.ToString("N");
            var sagaTypeName = typeof(SagaWithState).FullName!;

            var entity = await _dbContext.SagaStates
                .AsNoTracking()
                .Include(e => e.ProcessedMessages)
                .FirstOrDefaultAsync(
                    e => e.CorrelationId == correlationId && e.SagaType == sagaTypeName,
                    cancellationToken
                );

            if (entity is null)
                return Results.NotFound(new { orderId, status = "not-found" });

            MySagaState? state = null;
            if (entity.StateData is { Length: > 0 } payload)
            {
                state = _serializer.Deserialize(payload, typeof(MySagaState)) as MySagaState;
            }

            var response = new
            {
                orderId,
                sagaInstanceId = entity.InstanceId,
                completed = entity.IsCompleted,
                state = state is null
                    ? null
                    : new
                    {
                        state.Status,
                        state.Foo,
                        state.Bar,
                        state.LastUpdated
                    },
                processedMessages = entity.ProcessedMessages
                    .OrderBy(pm => pm.When)
                    .Select(pm => new { pm.MessageId, pm.When })
            };

            return Results.Ok(response);
        }
    }
}
