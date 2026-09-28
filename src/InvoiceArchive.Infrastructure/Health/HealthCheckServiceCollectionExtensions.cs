using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Infrastructure.Configuration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace InvoiceArchive.Infrastructure.Health;

public static class HealthCheckServiceCollectionExtensions
{
    public static IServiceCollection AddInvoiceArchiveHealthChecks(
        this IServiceCollection services,
        IConfiguration configuration,
        int port)
    {
        services.AddSingleton(new HealthCheckServerOptions { Port = port });

        services.AddSingleton<IHealthCheck, ElasticsearchHealthCheck>();
        services.AddSingleton<IHealthCheck, S3HealthCheck>();
        services.AddSingleton<IHealthCheck, KafkaHealthCheck>();

        var provider = configuration.GetSection(PersistenceOptions.SectionName)["Provider"];
        if (string.Equals(provider, "Postgres", StringComparison.OrdinalIgnoreCase))
        {
            services.AddSingleton<IHealthCheck, PostgresHealthCheck>();
        }

        services.AddHostedService<HealthCheckServer>();
        return services;
    }
}
