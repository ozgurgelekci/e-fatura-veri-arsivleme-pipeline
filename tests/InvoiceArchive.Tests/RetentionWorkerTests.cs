using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Infrastructure.Persistence;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.Tests;

public class RetentionWorkerTests
{
    [Fact]
    public async Task DryRun_calls_reaper_but_does_not_mutate_status()
    {
        var repo = new InMemoryArchiveBatchRepository();
        var batch = SeedVerifiedBatch(repo, "b1", completedAt: DateTime.UtcNow.AddDays(-40));

        var reaper = new RecordingReaper();
        var storage = new AlwaysExistsStorage();

        var worker = BuildWorker(repo, reaper, storage, dryRun: true, requireS3: true);

        await worker.RunOnceAsync(CancellationToken.None);

        Assert.Single(reaper.Calls);
        Assert.True(reaper.Calls[0].DryRun);

        var reloaded = await repo.FindAsync("b1", CancellationToken.None);
        Assert.Equal(ArchiveStatus.Verified, reloaded!.Status);
    }

    [Fact]
    public async Task Not_dry_run_marks_batch_as_deleted_after_reap()
    {
        var repo = new InMemoryArchiveBatchRepository();
        SeedVerifiedBatch(repo, "b2", completedAt: DateTime.UtcNow.AddDays(-40));

        var reaper = new RecordingReaper(matched: 5, deleted: 5);
        var storage = new AlwaysExistsStorage();

        var worker = BuildWorker(repo, reaper, storage, dryRun: false, requireS3: true);

        await worker.RunOnceAsync(CancellationToken.None);

        var reloaded = await repo.FindAsync("b2", CancellationToken.None);
        Assert.Equal(ArchiveStatus.Deleted, reloaded!.Status);
    }

    [Fact]
    public async Task Missing_s3_object_skips_reap_when_required()
    {
        var repo = new InMemoryArchiveBatchRepository();
        SeedVerifiedBatch(repo, "b3", completedAt: DateTime.UtcNow.AddDays(-40));

        var reaper = new RecordingReaper();
        var storage = new NeverExistsStorage();

        var worker = BuildWorker(repo, reaper, storage, dryRun: false, requireS3: true);

        await worker.RunOnceAsync(CancellationToken.None);

        Assert.Empty(reaper.Calls);
        var reloaded = await repo.FindAsync("b3", CancellationToken.None);
        Assert.Equal(ArchiveStatus.Verified, reloaded!.Status);
    }

    [Fact]
    public async Task Skips_batch_without_last_invoice_created_at()
    {
        var repo = new InMemoryArchiveBatchRepository();
        var batch = new ArchiveBatch
        {
            BatchId = "b4",
            CreatedAt = DateTime.UtcNow.AddDays(-90),
            Status = ArchiveStatus.Verified,
            CompletedAt = DateTime.UtcNow.AddDays(-40),
            FileName = "b4.zip",
            StoragePath = "2024/b4.zip",
            InvoiceCount = 10,
            LastInvoiceCreatedAt = null
        };
        await repo.SaveAsync(batch, CancellationToken.None);

        var reaper = new RecordingReaper();
        var worker = BuildWorker(repo, reaper, new AlwaysExistsStorage(), dryRun: false, requireS3: false);

        await worker.RunOnceAsync(CancellationToken.None);

        Assert.Empty(reaper.Calls);
    }

    [Fact]
    public async Task Skips_when_within_grace_period()
    {
        var repo = new InMemoryArchiveBatchRepository();
        SeedVerifiedBatch(repo, "b5", completedAt: DateTime.UtcNow.AddDays(-1));

        var reaper = new RecordingReaper();
        var worker = BuildWorker(repo, reaper, new AlwaysExistsStorage(),
            dryRun: false, requireS3: false, minAgeDays: 30);

        await worker.RunOnceAsync(CancellationToken.None);

        Assert.Empty(reaper.Calls);
    }

    private static ArchiveBatch SeedVerifiedBatch(
        InMemoryArchiveBatchRepository repo,
        string id,
        DateTime completedAt)
    {
        var batch = new ArchiveBatch
        {
            BatchId = id,
            CreatedAt = completedAt.AddMinutes(-5),
            Status = ArchiveStatus.Verified,
            CompletedAt = completedAt,
            FileName = $"{id}.zip",
            StoragePath = $"2024/{id}.zip",
            InvoiceCount = 5,
            FirstInvoiceCreatedAt = completedAt.AddHours(-2),
            LastInvoiceCreatedAt = completedAt.AddMinutes(-10)
        };
        repo.SaveAsync(batch, CancellationToken.None).GetAwaiter().GetResult();
        return batch;
    }

    private static InvoiceArchive.RetentionWorker.RetentionWorker BuildWorker(
        IArchiveBatchRepository repo,
        IArchivedInvoiceReaper reaper,
        IArchiveStorage storage,
        bool dryRun,
        bool requireS3,
        int minAgeDays = 30)
    {
        var options = new RetentionOptions
        {
            Enabled = true,
            IndexName = "invoices",
            BucketName = "invoice-archive",
            MinAgeAfterVerifiedDays = minAgeDays,
            PollIntervalMinutes = 60,
            BatchesPerRun = 50,
            RequireS3ObjectExists = requireS3,
            DryRun = dryRun
        };
        var monitor = new StaticOptionsMonitor<RetentionOptions>(options);

        return new InvoiceArchive.RetentionWorker.RetentionWorker(
            repo,
            reaper,
            storage,
            monitor,
            NullLogger<InvoiceArchive.RetentionWorker.RetentionWorker>.Instance,
            TimeProvider.System);
    }

    private sealed class RecordingReaper : IArchivedInvoiceReaper
    {
        private readonly long _matched;
        private readonly long _deleted;

        public RecordingReaper(long matched = 0, long deleted = 0)
        {
            _matched = matched;
            _deleted = deleted;
        }

        public List<(string BatchId, string IndexName, bool DryRun)> Calls { get; } = new();

        public Task<ReapResult> ReapAsync(ArchiveBatch batch, string indexName, bool dryRun, CancellationToken cancellationToken)
        {
            Calls.Add((batch.BatchId, indexName, dryRun));
            return Task.FromResult(new ReapResult(_matched, dryRun ? 0 : _deleted, dryRun));
        }
    }

    private sealed class AlwaysExistsStorage : IArchiveStorage
    {
        public string StorageName => "TestStorage";
        public Task<bool> ObjectExistsAsync(string bucket, string key, CancellationToken cancellationToken)
            => Task.FromResult(true);
        public Task UploadAsync(string bucket, string key, Stream content, long contentLength, string sha256,
            IReadOnlyDictionary<string, string>? metadata, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class NeverExistsStorage : IArchiveStorage
    {
        public string StorageName => "TestStorage";
        public Task<bool> ObjectExistsAsync(string bucket, string key, CancellationToken cancellationToken)
            => Task.FromResult(false);
        public Task UploadAsync(string bucket, string key, Stream content, long contentLength, string sha256,
            IReadOnlyDictionary<string, string>? metadata, CancellationToken cancellationToken)
            => Task.CompletedTask;
    }

    private sealed class StaticOptionsMonitor<T> : IOptionsMonitor<T>
    {
        public StaticOptionsMonitor(T value) { CurrentValue = value; }
        public T CurrentValue { get; }
        public T Get(string? name) => CurrentValue;
        public IDisposable? OnChange(Action<T, string?> listener) => null;
    }
}
