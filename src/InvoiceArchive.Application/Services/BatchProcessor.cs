using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.Application.Services;

public sealed class BatchProcessor
{
    private readonly IInvoiceReader _reader;
    private readonly ArchiveService _archiveService;
    private readonly IBatchIdGenerator _batchIdGenerator;
    private readonly IOptionsMonitor<ArchiveOptions> _options;
    private readonly ILogger<BatchProcessor> _logger;
    private readonly TimeProvider _time;

    public BatchProcessor(
        IInvoiceReader reader,
        ArchiveService archiveService,
        IBatchIdGenerator batchIdGenerator,
        IOptionsMonitor<ArchiveOptions> options,
        ILogger<BatchProcessor> logger,
        TimeProvider time)
    {
        _reader = reader;
        _archiveService = archiveService;
        _batchIdGenerator = batchIdGenerator;
        _options = options;
        _logger = logger;
        _time = time;
    }

    public async Task<int> RunAsync(CancellationToken cancellationToken)
    {
        var options = _options.CurrentValue;

        var query = new InvoiceQuery(
            options.IndexName,
            options.CreatedBeforeUtc,
            options.TenantId,
            options.ElasticsearchPageSize,
            TimeSpan.FromMinutes(options.PitKeepAliveMinutes));

        _logger.LogInformation(
            "Starting archive run. Index={Index} TenantId={TenantId} MaxRecordsPerBatch={MaxRecords} MaxArchiveSizeBytes={MaxBytes}",
            options.IndexName, options.TenantId, options.MaxRecordsPerBatch, options.MaxArchiveSizeBytes);

        var buffer = new BufferedInvoiceStream(_reader.ReadAsync(query, cancellationToken));
        long sequence = 0;
        var batchesCompleted = 0;

        while (!cancellationToken.IsCancellationRequested)
        {
            var peek = await buffer.PeekAsync(cancellationToken).ConfigureAwait(false);
            if (peek is null)
            {
                break;
            }

            sequence++;
            var batchId = _batchIdGenerator.NewBatchId(
                _time.GetUtcNow().UtcDateTime,
                options.TenantId,
                sequence);

            var invoices = buffer.TakeAsync(options.MaxRecordsPerBatch, cancellationToken);
            var batch = await _archiveService.ProcessBatchAsync(
                batchId,
                options.TenantId,
                invoices,
                cancellationToken).ConfigureAwait(false);

            if (batch is not null)
            {
                batchesCompleted++;
            }
        }

        _logger.LogInformation("Archive run completed. BatchesCompleted={Count}", batchesCompleted);
        return batchesCompleted;
    }
}
