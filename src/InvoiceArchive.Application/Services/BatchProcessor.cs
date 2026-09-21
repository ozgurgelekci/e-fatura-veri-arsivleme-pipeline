using System.Text;
using System.Threading.Channels;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Domain.Invoices;
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
        var limits = new BatchLimits(options.MaxRecordsPerBatch, options.MaxArchiveSizeBytes);
        var concurrency = Math.Max(1, options.MaxConcurrency);
        var channelCapacity = Math.Max(1, options.ChannelCapacity);

        var query = new InvoiceQuery(
            options.IndexName,
            options.CreatedBeforeUtc,
            options.TenantId,
            options.ElasticsearchPageSize,
            TimeSpan.FromMinutes(options.PitKeepAliveMinutes));

        _logger.LogInformation(
            "Starting archive run. Index={Index} TenantId={TenantId} MaxRecordsPerBatch={MaxRecords} MaxArchiveSizeBytes={MaxBytes} MaxConcurrency={Concurrency} ChannelCapacity={Capacity}",
            options.IndexName, options.TenantId, options.MaxRecordsPerBatch, options.MaxArchiveSizeBytes,
            concurrency, channelCapacity);

        var channel = Channel.CreateBounded<ArchiveJob>(new BoundedChannelOptions(channelCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = concurrency == 1,
            SingleWriter = true,
            AllowSynchronousContinuations = false
        });

        await using var buffer = new BufferedInvoiceStream(_reader.ReadAsync(query, cancellationToken));

        var producer = ProduceBatchesAsync(buffer, channel.Writer, options, limits, cancellationToken);

        var batchesCompleted = 0;
        var consumers = new Task[concurrency];
        for (var i = 0; i < concurrency; i++)
        {
            consumers[i] = ConsumeBatchesAsync(channel.Reader, () => Interlocked.Increment(ref batchesCompleted), cancellationToken);
        }

        try
        {
            await producer.ConfigureAwait(false);
        }
        catch
        {
            channel.Writer.TryComplete();
            await Task.WhenAll(consumers).ConfigureAwait(false);
            throw;
        }

        await Task.WhenAll(consumers).ConfigureAwait(false);

        _logger.LogInformation(
            "Archive run completed. BatchesCompleted={Count} Concurrency={Concurrency}",
            batchesCompleted, concurrency);
        return batchesCompleted;
    }

    private async Task ProduceBatchesAsync(
        BufferedInvoiceStream buffer,
        ChannelWriter<ArchiveJob> writer,
        ArchiveOptions options,
        BatchLimits limits,
        CancellationToken cancellationToken)
    {
        try
        {
            long sequence = 0;

            while (!cancellationToken.IsCancellationRequested)
            {
                var first = await buffer.TakeOneAsync(cancellationToken).ConfigureAwait(false);
                if (first is null)
                {
                    break;
                }

                sequence++;
                var batchId = _batchIdGenerator.NewBatchId(
                    _time.GetUtcNow().UtcDateTime,
                    options.TenantId,
                    sequence);

                var invoices = new List<Invoice>(Math.Min(1024, limits.MaxRecordCount)) { first };
                long approxSize = Encoding.UTF8.GetByteCount(first.Xml);

                while (invoices.Count < limits.MaxRecordCount && approxSize < limits.MaxArchiveSizeBytes)
                {
                    var next = await buffer.TakeOneAsync(cancellationToken).ConfigureAwait(false);
                    if (next is null)
                    {
                        break;
                    }
                    invoices.Add(next);
                    approxSize += Encoding.UTF8.GetByteCount(next.Xml);
                }

                var job = new ArchiveJob(batchId, options.TenantId, invoices);
                await writer.WriteAsync(job, cancellationToken).ConfigureAwait(false);
            }
        }
        finally
        {
            writer.TryComplete();
        }
    }

    private async Task ConsumeBatchesAsync(
        ChannelReader<ArchiveJob> reader,
        Action onBatchCompleted,
        CancellationToken cancellationToken)
    {
        await foreach (var job in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            var batch = await _archiveService.ProcessBatchAsync(
                job.BatchId,
                job.TenantId,
                ToAsyncEnumerable(job.Invoices, cancellationToken),
                cancellationToken).ConfigureAwait(false);

            if (batch is not null)
            {
                onBatchCompleted();
            }
        }
    }

    private static async IAsyncEnumerable<Invoice> ToAsyncEnumerable(
        IReadOnlyList<Invoice> invoices,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var invoice in invoices)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return invoice;
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }

    private sealed record ArchiveJob(string BatchId, string? TenantId, IReadOnlyList<Invoice> Invoices);
}
