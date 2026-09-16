using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Contracts.Events;
using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Domain.Invoices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.Application.Services;

public sealed class ArchiveService
{
    private readonly IZipArchiveBuilder _zipBuilder;
    private readonly IArchiveStorage _storage;
    private readonly IArchiveEventPublisher _eventPublisher;
    private readonly IArchiveBatchRepository _repository;
    private readonly IStorageKeyBuilder _keyBuilder;
    private readonly IOptionsMonitor<ArchiveOptions> _options;
    private readonly ILogger<ArchiveService> _logger;
    private readonly TimeProvider _time;

    public ArchiveService(
        IZipArchiveBuilder zipBuilder,
        IArchiveStorage storage,
        IArchiveEventPublisher eventPublisher,
        IArchiveBatchRepository repository,
        IStorageKeyBuilder keyBuilder,
        IOptionsMonitor<ArchiveOptions> options,
        ILogger<ArchiveService> logger,
        TimeProvider time)
    {
        _zipBuilder = zipBuilder;
        _storage = storage;
        _eventPublisher = eventPublisher;
        _repository = repository;
        _keyBuilder = keyBuilder;
        _options = options;
        _logger = logger;
        _time = time;
    }

    public async Task<ArchiveBatch?> ProcessBatchAsync(
        string batchId,
        string? tenantId,
        IAsyncEnumerable<Invoice> invoices,
        CancellationToken cancellationToken)
    {
        var options = _options.CurrentValue;
        var limits = new BatchLimits(options.MaxRecordsPerBatch, options.MaxArchiveSizeBytes);
        var nowUtc = _time.GetUtcNow().UtcDateTime;

        var batch = new ArchiveBatch
        {
            BatchId = batchId,
            TenantId = tenantId,
            CreatedAt = nowUtc,
            Status = ArchiveStatus.Archiving,
            FileName = _keyBuilder.FileName(batchId),
            StoragePath = _keyBuilder.Build(nowUtc, batchId, tenantId)
        };

        await _repository.SaveAsync(batch, cancellationToken).ConfigureAwait(false);

        var tempFile = Path.Combine(Path.GetTempPath(), $"invoice-archive-{batchId}.zip");
        ZipBuildResult buildResult;
        try
        {
            await using (var fileStream = new FileStream(
                tempFile,
                FileMode.Create,
                FileAccess.ReadWrite,
                FileShare.None,
                bufferSize: 81920,
                FileOptions.SequentialScan | FileOptions.Asynchronous))
            {
                buildResult = await _zipBuilder.BuildAsync(
                    batchId,
                    tenantId,
                    invoices,
                    fileStream,
                    limits,
                    cancellationToken).ConfigureAwait(false);
            }

            if (buildResult.InvoiceCount == 0)
            {
                _logger.LogInformation("BatchId={BatchId} No invoices in stream, skipping.", batchId);
                batch.Status = ArchiveStatus.Deleted;
                batch.CompletedAt = _time.GetUtcNow().UtcDateTime;
                await _repository.SaveAsync(batch, cancellationToken).ConfigureAwait(false);
                return null;
            }

            batch.InvoiceCount = buildResult.InvoiceCount;
            batch.SizeInBytes = buildResult.SizeInBytes;
            batch.Sha256 = buildResult.Sha256;
            batch.FirstInvoiceId = buildResult.FirstInvoiceId;
            batch.LastInvoiceId = buildResult.LastInvoiceId;

            var alreadyThere = await _storage.ObjectExistsAsync(
                options.BucketName,
                batch.StoragePath,
                cancellationToken).ConfigureAwait(false);

            if (alreadyThere)
            {
                _logger.LogInformation(
                    "BatchId={BatchId} Storage object already exists at {Bucket}/{Key}, idempotent skip.",
                    batchId, options.BucketName, batch.StoragePath);
            }
            else
            {
                await using var uploadStream = new FileStream(
                    tempFile,
                    FileMode.Open,
                    FileAccess.Read,
                    FileShare.Read,
                    bufferSize: 81920,
                    FileOptions.SequentialScan | FileOptions.Asynchronous);

                var metadata = new Dictionary<string, string>
                {
                    ["batch-id"] = batchId,
                    ["sha256"] = buildResult.Sha256,
                    ["invoice-count"] = buildResult.InvoiceCount.ToString()
                };
                if (!string.IsNullOrEmpty(tenantId))
                {
                    metadata["tenant-id"] = tenantId;
                }

                await _storage.UploadAsync(
                    options.BucketName,
                    batch.StoragePath,
                    uploadStream,
                    buildResult.SizeInBytes,
                    buildResult.Sha256,
                    metadata,
                    cancellationToken).ConfigureAwait(false);
            }

            batch.Status = ArchiveStatus.Uploaded;
            batch.CompletedAt = _time.GetUtcNow().UtcDateTime;
            await _repository.SaveAsync(batch, cancellationToken).ConfigureAwait(false);

            await _eventPublisher.PublishCreatedAsync(new ArchiveCreatedEvent
            {
                EventId = Guid.NewGuid().ToString("N"),
                BatchId = batchId,
                FileName = batch.FileName,
                Size = batch.SizeInBytes,
                InvoiceCount = batch.InvoiceCount,
                Storage = _storage.StorageName,
                Bucket = options.BucketName,
                Path = batch.StoragePath,
                Sha256 = buildResult.Sha256,
                CreatedAt = batch.CompletedAt.Value,
                TenantId = tenantId,
                FirstInvoiceId = buildResult.FirstInvoiceId,
                LastInvoiceId = buildResult.LastInvoiceId
            }, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation(
                "BatchId={BatchId} InvoiceCount={InvoiceCount} ZipSize={SizeBytes} Sha256={Sha256} Status=Completed",
                batchId, batch.InvoiceCount, batch.SizeInBytes, buildResult.Sha256);

            return batch;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            batch.Status = ArchiveStatus.Failed;
            batch.ErrorMessage = ex.Message;
            batch.CompletedAt = _time.GetUtcNow().UtcDateTime;
            await _repository.SaveAsync(batch, CancellationToken.None).ConfigureAwait(false);

            await _eventPublisher.PublishFailedAsync(new ArchiveFailedEvent
            {
                EventId = Guid.NewGuid().ToString("N"),
                BatchId = batchId,
                Reason = ex.Message,
                AttemptCount = batch.RetryCount + 1,
                FailedAt = batch.CompletedAt.Value,
                TenantId = tenantId
            }, CancellationToken.None).ConfigureAwait(false);

            _logger.LogError(ex, "BatchId={BatchId} archive failed", batchId);
            throw;
        }
        finally
        {
            TryDeleteTempFile(tempFile);
        }
    }

    private void TryDeleteTempFile(string path)
    {
        try
        {
            if (File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to delete temp file {Path}", path);
        }
    }
}
