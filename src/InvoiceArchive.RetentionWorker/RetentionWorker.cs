using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Domain.Archives;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.RetentionWorker;

public sealed class RetentionWorker : BackgroundService
{
    private readonly IArchiveBatchRepository _repository;
    private readonly IArchivedInvoiceReaper _reaper;
    private readonly IArchiveStorage _storage;
    private readonly IOptionsMonitor<RetentionOptions> _retentionOptions;
    private readonly ILogger<RetentionWorker> _logger;
    private readonly TimeProvider _time;

    public RetentionWorker(
        IArchiveBatchRepository repository,
        IArchivedInvoiceReaper reaper,
        IArchiveStorage storage,
        IOptionsMonitor<RetentionOptions> retentionOptions,
        ILogger<RetentionWorker> logger,
        TimeProvider time)
    {
        _repository = repository;
        _reaper = reaper;
        _storage = storage;
        _retentionOptions = retentionOptions;
        _logger = logger;
        _time = time;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var options = _retentionOptions.CurrentValue;
        if (!options.Enabled)
        {
            _logger.LogInformation(
                "Retention worker is disabled (Retention:Enabled=false). Idling until process stops.");
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            return;
        }

        _logger.LogInformation(
            "Retention worker starting. DryRun={DryRun} MinAgeAfterVerifiedDays={MinAge} PollIntervalMinutes={Interval}",
            options.DryRun, options.MinAgeAfterVerifiedDays, options.PollIntervalMinutes);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunOnceAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Retention scan failed; will retry after poll interval.");
            }

            var interval = TimeSpan.FromMinutes(_retentionOptions.CurrentValue.PollIntervalMinutes);
            try
            {
                await Task.Delay(interval, stoppingToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    public async Task RunOnceAsync(CancellationToken cancellationToken)
    {
        var options = _retentionOptions.CurrentValue;

        var cutoff = _time.GetUtcNow().UtcDateTime - TimeSpan.FromDays(options.MinAgeAfterVerifiedDays);

        var candidates = await _repository.FindByStatusCompletedBeforeAsync(
            ArchiveStatus.Verified,
            cutoff,
            options.BatchesPerRun,
            cancellationToken).ConfigureAwait(false);

        if (candidates.Count == 0)
        {
            _logger.LogDebug("Retention scan: no candidates before {Cutoff:o}", cutoff);
            return;
        }

        _logger.LogInformation(
            "Retention scan: {Count} candidate batch(es) before {Cutoff:o}",
            candidates.Count, cutoff);

        foreach (var batch in candidates)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!batch.LastInvoiceCreatedAt.HasValue)
            {
                _logger.LogWarning(
                    "BatchId={BatchId} skipped: LastInvoiceCreatedAt is null (pre-retention batch).",
                    batch.BatchId);
                continue;
            }

            if (options.RequireS3ObjectExists)
            {
                var exists = await _storage.ObjectExistsAsync(
                    options.BucketName,
                    batch.StoragePath,
                    cancellationToken).ConfigureAwait(false);

                if (!exists)
                {
                    _logger.LogWarning(
                        "BatchId={BatchId} skipped: archive object missing at {Bucket}/{Path}.",
                        batch.BatchId, options.BucketName, batch.StoragePath);
                    continue;
                }
            }

            var result = await _reaper.ReapAsync(
                batch,
                options.IndexName,
                options.DryRun,
                cancellationToken).ConfigureAwait(false);

            if (result.DryRun)
            {
                continue;
            }

            batch.Status = ArchiveStatus.Deleted;
            batch.CompletedAt = _time.GetUtcNow().UtcDateTime;
            await _repository.SaveAsync(batch, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation(
                "BatchId={BatchId} marked Deleted after reaping {DeletedCount} document(s).",
                batch.BatchId, result.DeletedCount);
        }
    }
}
