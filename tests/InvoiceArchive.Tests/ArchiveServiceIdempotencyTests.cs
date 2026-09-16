using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Application.Services;
using InvoiceArchive.Domain.Invoices;
using InvoiceArchive.Infrastructure.Persistence;
using InvoiceArchive.Infrastructure.Zip;
using InvoiceArchive.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.Tests;

public class ArchiveServiceIdempotencyTests
{
    [Fact]
    public async Task Second_upload_is_skipped_when_object_already_exists()
    {
        var options = new OptionsMonitorStub<ArchiveOptions>(new ArchiveOptions
        {
            MaxRecordsPerBatch = 100,
            MaxArchiveSizeBytes = 10 * 1024 * 1024,
            BucketName = "test-bucket"
        });

        var storage = new FakeStorage();
        var publisher = new FakeEventPublisher();
        var repository = new InMemoryArchiveBatchRepository();
        var keyBuilder = new DefaultStorageKeyBuilder();
        var zipBuilder = new StreamingZipArchiveBuilder(
            NullLogger<StreamingZipArchiveBuilder>.Instance, TimeProvider.System);

        var service = new ArchiveService(
            zipBuilder, storage, publisher, repository, keyBuilder,
            options, NullLogger<ArchiveService>.Instance, TimeProvider.System);

        var batch1 = await service.ProcessBatchAsync("batch-1", null, ProduceInvoices(5), CancellationToken.None);
        Assert.NotNull(batch1);
        Assert.Equal(1, storage.UploadCallCount);

        var batch2 = await service.ProcessBatchAsync("batch-1", null, ProduceInvoices(5), CancellationToken.None);
        Assert.NotNull(batch2);
        Assert.Equal(1, storage.UploadCallCount);
        Assert.Equal(2, publisher.Created.Count);
    }

    private static async IAsyncEnumerable<Invoice> ProduceInvoices(int count)
    {
        for (var i = 1; i <= count; i++)
        {
            yield return new Invoice
            {
                InvoiceId = $"INV-{i}",
                Xml = $"<Invoice id=\"{i}\">payload</Invoice>",
                CreatedAt = new DateTime(2026, 1, 1, 0, 0, i, DateTimeKind.Utc)
            };
            await Task.Yield();
        }
    }
}

internal sealed class OptionsMonitorStub<T> : IOptionsMonitor<T>
{
    public OptionsMonitorStub(T value) => CurrentValue = value;

    public T CurrentValue { get; }
    public T Get(string? name) => CurrentValue;
    public IDisposable OnChange(Action<T, string?> listener) => NullDisposable.Instance;

    private sealed class NullDisposable : IDisposable
    {
        public static readonly NullDisposable Instance = new();
        public void Dispose() { }
    }
}
