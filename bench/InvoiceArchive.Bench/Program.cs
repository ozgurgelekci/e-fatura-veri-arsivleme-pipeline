using System.Diagnostics;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Application.Services;
using InvoiceArchive.Bench.Fakes;
using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Infrastructure.Persistence;
using InvoiceArchive.Infrastructure.Zip;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

var mode = args.FirstOrDefault() ?? "sweep";

switch (mode)
{
    case "zip":
    {
        var invoiceCount = ArgAsInt(args, 1, 5_000);
        var xmlBytes = ArgAsInt(args, 2, 4_096);
        var includeMetadata = ArgAsBool(args, 3, true);
        await RunZipAsync(invoiceCount, xmlBytes, includeMetadata);
        return 0;
    }
    case "pipeline":
    {
        var invoiceCount = ArgAsInt(args, 1, 10_000);
        var concurrency = ArgAsInt(args, 2, 1);
        var channelCapacity = ArgAsInt(args, 3, 4);
        var uploadDelayMs = ArgAsInt(args, 4, 0);
        var maxRecordsPerBatch = ArgAsInt(args, 5, 1_000);
        await RunPipelineAsync(invoiceCount, concurrency, channelCapacity, uploadDelayMs, maxRecordsPerBatch);
        return 0;
    }
    case "sweep":
    {
        var invoiceCount = ArgAsInt(args, 1, 10_000);
        var uploadDelayMs = ArgAsInt(args, 2, 5);
        var maxRecordsPerBatch = ArgAsInt(args, 3, 1_000);
        await RunSweepAsync(invoiceCount, uploadDelayMs, maxRecordsPerBatch);
        return 0;
    }
    case "--help":
    case "-h":
        PrintUsage();
        return 0;
    default:
        Console.Error.WriteLine($"Unknown mode: {mode}");
        PrintUsage();
        return 2;
}

static async Task RunZipAsync(int invoiceCount, int xmlBytes, bool includeMetadata)
{
    Console.WriteLine($"[zip] count={invoiceCount} xmlBytes={xmlBytes} metadata={includeMetadata}");

    var builder = new StreamingZipArchiveBuilder(
        NullLogger<StreamingZipArchiveBuilder>.Instance, TimeProvider.System);
    var reader = new GeneratedInvoiceReader(invoiceCount, xmlBytes);
    var query = new InvoiceQuery("bench", null, null, invoiceCount, TimeSpan.FromMinutes(1));

    // Warm-up
    using (var warm = new MemoryStream())
    {
        await builder.BuildAsync(
            "warm", null, reader.ReadAsync(query, CancellationToken.None), warm,
            new BatchLimits(invoiceCount, long.MaxValue),
            includeMetadata, CancellationToken.None);
    }

    var sw = Stopwatch.StartNew();
    long sizeBytes;
    int actual;
    using (var memory = new MemoryStream())
    {
        var result = await builder.BuildAsync(
            "bench", "tenant-bench",
            reader.ReadAsync(query, CancellationToken.None),
            memory,
            new BatchLimits(invoiceCount, long.MaxValue),
            includeMetadata, CancellationToken.None);
        sizeBytes = result.SizeInBytes;
        actual = result.InvoiceCount;
    }
    sw.Stop();

    var mib = sizeBytes / 1024.0 / 1024.0;
    var msPerInv = sw.Elapsed.TotalMilliseconds / actual;
    var mibPerSec = mib / sw.Elapsed.TotalSeconds;
    Console.WriteLine(
        $"[zip] invoices={actual} zipSize={mib:F2} MiB elapsed={sw.Elapsed.TotalSeconds:F3}s " +
        $"throughput={mibPerSec:F2} MiB/s perInvoice={msPerInv:F3} ms");
}

static async Task RunPipelineAsync(
    int invoiceCount, int concurrency, int channelCapacity, int uploadDelayMs, int maxRecordsPerBatch)
{
    Console.WriteLine(
        $"[pipeline] count={invoiceCount} concurrency={concurrency} channel={channelCapacity} " +
        $"uploadDelayMs={uploadDelayMs} maxRecordsPerBatch={maxRecordsPerBatch}");

    var (elapsed, batches, uploadedBytes) = await ExecutePipelineAsync(
        invoiceCount, concurrency, channelCapacity, uploadDelayMs, maxRecordsPerBatch, xmlBytes: 4_096);

    var mib = uploadedBytes / 1024.0 / 1024.0;
    var invPerSec = invoiceCount / Math.Max(elapsed.TotalSeconds, 0.0001);
    Console.WriteLine(
        $"[pipeline] elapsed={elapsed.TotalSeconds:F3}s batches={batches} zipTotal={mib:F2} MiB " +
        $"throughput={invPerSec:F0} inv/s");
}

static async Task RunSweepAsync(int invoiceCount, int uploadDelayMs, int maxRecordsPerBatch)
{
    Console.WriteLine(
        $"[sweep] count={invoiceCount} uploadDelayMs={uploadDelayMs} maxRecordsPerBatch={maxRecordsPerBatch}");
    Console.WriteLine();
    Console.WriteLine("| Concurrency | Channel | Batches | Elapsed (s) | inv/s   | MiB/s |");
    Console.WriteLine("|-------------|---------|---------|-------------|---------|-------|");

    int[] concurrencies = { 1, 2, 4, 8 };
    int[] channels = { 1, 4, 16 };

    // Warm-up
    await ExecutePipelineAsync(Math.Min(invoiceCount, 1_000), 1, 1, 0, maxRecordsPerBatch, 4_096);

    foreach (var c in concurrencies)
    {
        foreach (var ch in channels)
        {
            var (elapsed, batches, uploadedBytes) = await ExecutePipelineAsync(
                invoiceCount, c, ch, uploadDelayMs, maxRecordsPerBatch, xmlBytes: 4_096);
            var invPerSec = invoiceCount / Math.Max(elapsed.TotalSeconds, 0.0001);
            var mibPerSec = uploadedBytes / 1024.0 / 1024.0 / Math.Max(elapsed.TotalSeconds, 0.0001);
            Console.WriteLine(
                $"| {c,11} | {ch,7} | {batches,7} | {elapsed.TotalSeconds,11:F3} | {invPerSec,7:F0} | {mibPerSec,5:F1} |");
        }
    }
}

static async Task<(TimeSpan Elapsed, int Batches, long UploadedBytes)> ExecutePipelineAsync(
    int invoiceCount, int concurrency, int channelCapacity, int uploadDelayMs, int maxRecordsPerBatch, int xmlBytes)
{
    var options = new ArchiveOptions
    {
        IndexName = "bench",
        MaxRecordsPerBatch = maxRecordsPerBatch,
        MaxArchiveSizeBytes = long.MaxValue,
        ElasticsearchPageSize = 1_000,
        PitKeepAliveMinutes = 5,
        MaxConcurrency = concurrency,
        ChannelCapacity = channelCapacity,
        BucketName = "bench-bucket",
        RunOnceAndExit = true,
        IncludeInvoiceMetadata = true
    };
    var monitor = new StaticOptionsMonitor<ArchiveOptions>(options);

    var reader = new GeneratedInvoiceReader(invoiceCount, xmlBytes);
    var storage = new NoopStorage(TimeSpan.FromMilliseconds(uploadDelayMs));
    var publisher = new NoopEventPublisher();
    var repo = new InMemoryArchiveBatchRepository();
    var keyBuilder = new DefaultStorageKeyBuilder();
    var idGenerator = new DefaultBatchIdGenerator();
    var zipBuilder = new StreamingZipArchiveBuilder(
        NullLogger<StreamingZipArchiveBuilder>.Instance, TimeProvider.System);

    var archiveService = new ArchiveService(
        zipBuilder, storage, publisher, repo, keyBuilder, NullArchiveMetrics.Instance,
        monitor, NullLogger<ArchiveService>.Instance, TimeProvider.System);

    var batchProcessor = new BatchProcessor(
        reader, archiveService, idGenerator, monitor,
        NullLogger<BatchProcessor>.Instance, TimeProvider.System);

    var sw = Stopwatch.StartNew();
    var batches = await batchProcessor.RunAsync(CancellationToken.None);
    sw.Stop();

    return (sw.Elapsed, batches, storage.UploadedBytes);
}

static int ArgAsInt(string[] args, int index, int fallback)
    => args.Length > index && int.TryParse(args[index], out var v) ? v : fallback;

static bool ArgAsBool(string[] args, int index, bool fallback)
    => args.Length > index && bool.TryParse(args[index], out var v) ? v : fallback;

static void PrintUsage()
{
    Console.WriteLine(
        """
        Usage:
          bench sweep      [invoiceCount=10000] [uploadDelayMs=5] [maxRecordsPerBatch=1000]
              Runs pipeline for a 4x3 (concurrency x channel) matrix and prints a table.

          bench pipeline   <invoiceCount> <concurrency> <channelCapacity> [uploadDelayMs=0] [maxRecordsPerBatch=1000]
              Runs BatchProcessor with fake reader/storage/publisher.

          bench zip        <invoiceCount> <xmlBytes> [includeMetadata=true]
              Measures pure ZIP builder throughput and per-invoice cost.

        Notes:
          - Storage/publisher/repository are in-process fakes; no ES/S3/Kafka required.
          - uploadDelayMs simulates network round-trip per batch (Task.Delay).
        """);
}

file sealed class StaticOptionsMonitor<T> : IOptionsMonitor<T>
{
    public StaticOptionsMonitor(T value) { CurrentValue = value; }
    public T CurrentValue { get; }
    public T Get(string? name) => CurrentValue;
    public IDisposable? OnChange(Action<T, string?> listener) => null;
}
