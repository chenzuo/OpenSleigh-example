using OpenSleigh.DependencyInjection;
using OpenSleigh.InMemory;
using OpenSleigh.Outbox;
using OpenSleigh.Persistence.SQL;
using OpenSleigh.Persistence.SQLServer;
using OpenSleigh.Transport;
using WebApplication7.Domain.Sagas;
using WebApplication7.Domain.States;

namespace WebApplication7.Infrastructure
{
    public static class OpenSleighSqlServerConfig
    {
        public static IServiceCollection AddOpenSleighSqlServer(
            this IServiceCollection services,
            IConfiguration configuration
        )
        {
            var connectionString = configuration.GetConnectionString("SagaSql");
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new InvalidOperationException("Connection string 'SagaSql' was not found.");

            services.AddOpenSleigh(cfg =>
            {
                var sqlConfiguration = new SqlConfiguration(connectionString);
                cfg.UseSqlServerPersistence(sqlConfiguration).UseInMemoryTransport();
                cfg.AddSaga<SagaWithState, MySagaState>();
                cfg.AddSaga<RefundSaga, RefundSagaState>();
                cfg.WithOutboxProcessorOptions(OutboxProcessorOptions.Default);
            });

            services.AddSingleton<IMessageProcessor, ScopedMessageProcessor>();
            services.AddScoped<ISagaStateReader, SagaStateReader>();

            return services;
        }
    }
}
