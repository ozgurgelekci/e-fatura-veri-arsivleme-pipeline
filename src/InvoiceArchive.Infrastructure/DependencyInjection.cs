using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Services;
using InvoiceArchive.Infrastructure.Configuration;
using InvoiceArchive.Infrastructure.Elasticsearch;
using InvoiceArchive.Infrastructure.Kafka;
using InvoiceArchive.Infrastructure.Persistence;
using InvoiceArchive.Infrastructure.Retention;
using InvoiceArchive.Infrastructure.Storage;
using InvoiceArchive.Infrastructure.Zip;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Npgsql;

namespace InvoiceArchive.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddInvoiceArchiveInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddOptions<ElasticsearchOptions>()
            .Bind(configuration.GetSection(ElasticsearchOptions.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddOptions<StorageOptions>()
            .Bind(configuration.GetSection(StorageOptions.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(KafkaOptions.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddOptions<PersistenceOptions>()
            .Bind(configuration.GetSection(PersistenceOptions.SectionName))
            .ValidateDataAnnotations()
            .ValidateOnStart();

        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<ElasticsearchClientFactory>();
        services.AddSingleton<IInvoiceReader, ElasticsearchInvoiceReader>();
        services.AddSingleton<IZipArchiveBuilder, StreamingZipArchiveBuilder>();
        services.AddSingleton<IArchiveStorage, S3ArchiveStorage>();
        services.AddSingleton<IArchiveEventPublisher, KafkaArchiveEventPublisher>();
        services.AddSingleton<IBatchIdGenerator, DefaultBatchIdGenerator>();
        services.AddSingleton<IStorageKeyBuilder, DefaultStorageKeyBuilder>();
        services.TryAddSingleton<IArchiveMetrics>(NullArchiveMetrics.Instance);
        services.AddSingleton<ArchiveService>();
        services.AddSingleton<BatchProcessor>();
        services.AddSingleton<KafkaArchiveEventConsumer>();
        services.AddSingleton<IArchivedInvoiceReaper, ElasticsearchInvoiceReaper>();

        AddArchiveBatchRepository(services, configuration);

        return services;
    }

    private static void AddArchiveBatchRepository(IServiceCollection services, IConfiguration configuration)
    {
        var provider = configuration.GetSection(PersistenceOptions.SectionName)["Provider"]
            ?? "InMemory";

        if (string.Equals(provider, "Postgres", StringComparison.OrdinalIgnoreCase))
        {
            services.AddSingleton<NpgsqlDataSource>(sp =>
            {
                var opts = sp.GetRequiredService<IOptions<PersistenceOptions>>().Value;
                if (string.IsNullOrWhiteSpace(opts.ConnectionString))
                {
                    throw new InvalidOperationException(
                        "Persistence:Provider=Postgres requires Persistence:ConnectionString to be set.");
                }
                return new NpgsqlDataSourceBuilder(opts.ConnectionString).Build();
            });
            services.AddSingleton<IArchiveBatchRepository, PostgresArchiveBatchRepository>();
        }
        else
        {
            services.AddSingleton<IArchiveBatchRepository, InMemoryArchiveBatchRepository>();
        }
    }
}
