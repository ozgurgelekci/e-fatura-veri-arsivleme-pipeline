using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Contracts.Events;
using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Infrastructure.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace InvoiceArchive.StorageConsumer;

public sealed class StorageStatusWorker : BackgroundService
{
    private readonly KafkaArchiveEventConsumer _consumer;
    private readonly IArchiveStorage _storage;
    private readonly IArchiveBatchRepository _repository;
    private readonly IArchiveEventPublisher _publisher;
    private readonly ILogger<StorageStatusWorker> _logger;
    private readonly TimeProvider _time;

    public StorageStatusWorker(
        KafkaArchiveEventConsumer consumer,
        IArchiveStorage storage,
        IArchiveBatchRepository repository,
        IArchiveEventPublisher publisher,
        ILogger<StorageStatusWorker> logger,
        TimeProvider time)
    {
        _consumer = consumer;
        _storage = storage;
        _repository = repository;
        _publisher = publisher;
        _logger = logger;
        _time = time;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("StorageStatusWorker starting.");
        await _consumer.ConsumeAsync(HandleAsync, stoppingToken).ConfigureAwait(false);
    }

    private async Task HandleAsync(ArchiveCreatedEvent evt, CancellationToken cancellationToken)
    {
        var exists = await _storage.ObjectExistsAsync(evt.Bucket, evt.Path, cancellationToken).ConfigureAwait(false);
        if (!exists)
        {
            throw new InvalidOperationException(
                $"Archive object missing at {evt.Bucket}/{evt.Path} for batch {evt.BatchId}");
        }

        var batch = await _repository.FindAsync(evt.BatchId, cancellationToken).ConfigureAwait(false);
        batch ??= new ArchiveBatch
        {
            BatchId = evt.BatchId,
            TenantId = evt.TenantId,
            CreatedAt = evt.CreatedAt,
            FileName = evt.FileName,
            StoragePath = evt.Path,
            InvoiceCount = evt.InvoiceCount,
            SizeInBytes = evt.Size,
            Sha256 = evt.Sha256
        };

        batch.Status = ArchiveStatus.Verified;
        batch.CompletedAt = _time.GetUtcNow().UtcDateTime;
        await _repository.SaveAsync(batch, cancellationToken).ConfigureAwait(false);

        await _publisher.PublishCompletedAsync(new ArchiveCompletedEvent
        {
            EventId = Guid.NewGuid().ToString("N"),
            BatchId = evt.BatchId,
            CompletedAt = batch.CompletedAt.Value,
            Path = evt.Path,
            Sha256 = evt.Sha256,
            TenantId = evt.TenantId
        }, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation("Verified BatchId={BatchId} at {Bucket}/{Path}", evt.BatchId, evt.Bucket, evt.Path);
    }
}
