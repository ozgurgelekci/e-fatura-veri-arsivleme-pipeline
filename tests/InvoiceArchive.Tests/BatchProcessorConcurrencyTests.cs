using System.Collections.Concurrent;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Application.Services;
using InvoiceArchive.Domain.Invoices;
using InvoiceArchive.Infrastructure.Persistence;
using InvoiceArchive.Infrastructure.Zip;
using InvoiceArchive.Tests.Fakes;
using Microsoft.Extensions.Logging.Abstractions;

namespace InvoiceArchive.Tests;

public class BatchProcessorConcurrencyTests
{
    [Fact]
    public async Task Processes_all_batches_with_multiple_consumers()
    {
        var options = new OptionsMonitorStub<ArchiveOptions>(new ArchiveOptions
        {
            MaxRecordsPerBatch = 10,
            MaxArchiveSizeBytes = 10 * 1024 * 1024,
            MaxConcurrency = 4,
            ChannelCapacity = 2,
            BucketName = "test-bucket"
        });

        var storage = new FakeStorage();
        var publisher = new FakeEventPublisher();
        var repository = new InMemoryArchiveBatchRepository();
        var keyBuilder = new DefaultStorageKeyBuilder();
        var zipBuilder = new StreamingZipArchiveBuilder(
            NullLogger<StreamingZipArchiveBuilder>.Instance, TimeProvider.System);

        var archiveService = new ArchiveService(
            zipBuilder, storage, publisher, repository, keyBuilder,
            options, NullLogger<ArchiveService>.Instance, TimeProvider.System);

        var reader = new FakeInvoiceReader(totalInvoices: 100, delayPerInvoice: TimeSpan.Zero);
        var processor = new BatchProcessor(
            reader,
            archiveService,
            new DefaultBatchIdGenerator(),
            options,
            NullLogger<BatchProcessor>.Instance,
            TimeProvider.System);

        var completed = await processor.RunAsync(CancellationToken.None);

        Assert.Equal(10, completed);
        Assert.Equal(10, storage.UploadCallCount);
        Assert.Equal(10, publisher.Created.Count);
        Assert.Equal(100, publisher.Created.Sum(e => e.InvoiceCount));
    }

    [Fact]
    public async Task Producer_stops_batch_when_approx_size_limit_reached()
    {
        var largeXml = new string('X', 2 * 1024 * 1024); // ~2 MB per invoice

        var options = new OptionsMonitorStub<ArchiveOptions>(new ArchiveOptions
        {
            MaxRecordsPerBatch = 1000,
            MaxArchiveSizeBytes = 5 * 1024 * 1024, // 5 MB
            MaxConcurrency = 1,
            ChannelCapacity = 1,
            BucketName = "test-bucket"
        });

        var storage = new FakeStorage();
        var publisher = new FakeEventPublisher();
        var repository = new InMemoryArchiveBatchRepository();
        var keyBuilder = new DefaultStorageKeyBuilder();
        var zipBuilder = new StreamingZipArchiveBuilder(
            NullLogger<StreamingZipArchiveBuilder>.Instance, TimeProvider.System);

        var archiveService = new ArchiveService(
            zipBuilder, storage, publisher, repository, keyBuilder,
            options, NullLogger<ArchiveService>.Instance, TimeProvider.System);

        var reader = new FakeInvoiceReader(totalInvoices: 10, xmlOverride: largeXml);
        var processor = new BatchProcessor(
            reader,
            archiveService,
            new DefaultBatchIdGenerator(),
            options,
            NullLogger<BatchProcessor>.Instance,
            TimeProvider.System);

        var completed = await processor.RunAsync(CancellationToken.None);

        Assert.True(completed >= 3, $"Expected at least 3 batches, got {completed}");
        Assert.All(publisher.Created, e => Assert.InRange(e.InvoiceCount, 1, 3));
        Assert.Equal(10, publisher.Created.Sum(e => e.InvoiceCount));
    }

    private sealed class FakeInvoiceReader : IInvoiceReader
    {
        private readonly int _totalInvoices;
        private readonly TimeSpan _delayPerInvoice;
        private readonly string? _xmlOverride;

        public FakeInvoiceReader(int totalInvoices, TimeSpan delayPerInvoice = default, string? xmlOverride = null)
        {
            _totalInvoices = totalInvoices;
            _delayPerInvoice = delayPerInvoice;
            _xmlOverride = xmlOverride;
        }

        public async IAsyncEnumerable<Invoice> ReadAsync(
            InvoiceQuery query,
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
        {
            for (var i = 1; i <= _totalInvoices; i++)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (_delayPerInvoice > TimeSpan.Zero)
                {
                    await Task.Delay(_delayPerInvoice, cancellationToken).ConfigureAwait(false);
                }
                yield return new Invoice
                {
                    InvoiceId = $"INV-{i:D6}",
                    Xml = _xmlOverride ?? $"<Invoice id=\"{i}\">payload-{i}</Invoice>",
                    CreatedAt = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddSeconds(i)
                };
            }
        }
    }
}
