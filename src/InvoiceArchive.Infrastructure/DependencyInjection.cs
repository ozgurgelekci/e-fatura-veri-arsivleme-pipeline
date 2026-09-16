using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Services;
using InvoiceArchive.Infrastructure.Configuration;
using InvoiceArchive.Infrastructure.Elasticsearch;
using InvoiceArchive.Infrastructure.Kafka;
using InvoiceArchive.Infrastructure.Persistence;
using InvoiceArchive.Infrastructure.Storage;
using InvoiceArchive.Infrastructure.Zip;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace InvoiceArchive.Infrastructure;

public static class DependencyInjection
{
    public static IServiceCollection AddInvoiceArchiveInfrastructure(
        this IServiceCollection services,
        IConfiguration configuration)
    {
        services.AddOptions<ElasticsearchOptions>()
            .Bind(configuration.GetSection(ElasticsearchOptions.SectionName))
            .ValidateOnStart();

        services.AddOptions<StorageOptions>()
            .Bind(configuration.GetSection(StorageOptions.SectionName))
            .ValidateOnStart();

        services.AddOptions<KafkaOptions>()
            .Bind(configuration.GetSection(KafkaOptions.SectionName))
            .ValidateOnStart();

        services.AddSingleton(TimeProvider.System);
        services.AddSingleton<ElasticsearchClientFactory>();
        services.AddSingleton<IInvoiceReader, ElasticsearchInvoiceReader>();
        services.AddSingleton<IZipArchiveBuilder, StreamingZipArchiveBuilder>();
        services.AddSingleton<IArchiveStorage, S3ArchiveStorage>();
        services.AddSingleton<IArchiveEventPublisher, KafkaArchiveEventPublisher>();
        services.AddSingleton<IArchiveBatchRepository, InMemoryArchiveBatchRepository>();
        services.AddSingleton<IBatchIdGenerator, DefaultBatchIdGenerator>();
        services.AddSingleton<IStorageKeyBuilder, DefaultStorageKeyBuilder>();
        services.AddSingleton<ArchiveService>();
        services.AddSingleton<BatchProcessor>();
        services.AddSingleton<KafkaArchiveEventConsumer>();

        return services;
    }
}
