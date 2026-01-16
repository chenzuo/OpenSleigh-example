using Microsoft.EntityFrameworkCore;
using OpenSleigh.Persistence.SQL;
using OpenSleigh.Utils;

namespace WebApplication7.Infrastructure
{
    public interface ISagaStateReader
    {
        Task<SagaStateView<TState>?> GetStateAsync<TSaga, TState>(
            string correlationId,
            CancellationToken cancellationToken
        )
            where TSaga : class
            where TState : class;
    }

    public sealed record ProcessedMessageView(string MessageId, DateTimeOffset When);

    public sealed record SagaStateView<TState>(
        string CorrelationId,
        string SagaInstanceId,
        bool Completed,
        IReadOnlyCollection<ProcessedMessageView> ProcessedMessages,
        TState? State
    )
        where TState : class;

    public class SagaStateReader : ISagaStateReader
    {
        private readonly SagaDbContext _dbContext;
        private readonly ISerializer _serializer;
        private readonly ILogger<SagaStateReader> _logger;

        public SagaStateReader(
            SagaDbContext dbContext,
            ISerializer serializer,
            ILogger<SagaStateReader> logger
        )
        {
            _dbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task<SagaStateView<TState>?> GetStateAsync<TSaga, TState>(
            string correlationId,
            CancellationToken cancellationToken
        )
            where TSaga : class
            where TState : class
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(correlationId);

            var sagaTypeName = typeof(TSaga).FullName!;

            if (_logger.IsEnabled(LogLevel.Debug))
                _logger.LogDebug(
                    "Fetching saga state: CorrelationId={CorrelationId}, SagaType={SagaType}",
                    correlationId,
                    sagaTypeName
                );

            var entity = await _dbContext
                .SagaStates.AsNoTracking()
                .Include(e => e.ProcessedMessages)
                .FirstOrDefaultAsync(
                    e => e.CorrelationId == correlationId && e.SagaType == sagaTypeName,
                    cancellationToken
                );

            if (entity is null)
            {
                _logger.LogDebug(
                    "Saga state not found: CorrelationId={CorrelationId}, SagaType={SagaType}",
                    correlationId,
                    sagaTypeName
                );
                return null;
            }

            TState? state = null;
            if (entity.StateData is { Length: > 0 })
                state = _serializer.Deserialize(entity.StateData, typeof(TState)) as TState;

            var processed = entity
                .ProcessedMessages.OrderBy(pm => pm.When)
                .Select(pm => new ProcessedMessageView(pm.MessageId, pm.When))
                .ToArray();

            return new SagaStateView<TState>(
                correlationId,
                entity.InstanceId,
                entity.IsCompleted,
                processed,
                state
            );
        }
    }
}
