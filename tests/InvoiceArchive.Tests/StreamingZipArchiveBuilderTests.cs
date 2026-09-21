using System.IO.Compression;
using System.Text;
using System.Text.Json;
using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Domain.Invoices;
using InvoiceArchive.Infrastructure.Zip;
using Microsoft.Extensions.Logging.Abstractions;

namespace InvoiceArchive.Tests;

public class StreamingZipArchiveBuilderTests
{
    [Fact]
    public async Task Builds_zip_with_manifest_and_counts_correctly()
    {
        var builder = new StreamingZipArchiveBuilder(
            NullLogger<StreamingZipArchiveBuilder>.Instance,
            TimeProvider.System);

        var invoices = ProduceInvoices(50);
        using var memory = new MemoryStream();
        var result = await builder.BuildAsync(
            "test-batch-1",
            tenantId: "tenant-a",
            invoices,
            memory,
            new BatchLimits(MaxRecordCount: 1000, MaxArchiveSizeBytes: long.MaxValue),
            CancellationToken.None);

        Assert.Equal(50, result.InvoiceCount);
        Assert.Equal("INV-1", result.FirstInvoiceId);
        Assert.Equal("INV-50", result.LastInvoiceId);
        Assert.False(result.LimitReached);
        Assert.False(string.IsNullOrEmpty(result.Sha256));

        memory.Position = 0;
        using var archive = new ZipArchive(memory, ZipArchiveMode.Read);
        Assert.Equal(51, archive.Entries.Count); // 50 invoices + manifest

        var manifestEntry = archive.GetEntry("manifest.json");
        Assert.NotNull(manifestEntry);
        using var manifestStream = manifestEntry!.Open();
        var manifest = await JsonSerializer.DeserializeAsync<ArchiveManifest>(manifestStream);
        Assert.NotNull(manifest);
        Assert.Equal("test-batch-1", manifest!.BatchId);
        Assert.Equal(50, manifest.InvoiceCount);
        Assert.Equal("tenant-a", manifest.TenantId);
        Assert.Equal("deflate", manifest.Compression);
        Assert.Equal(1, manifest.ArchiveVersion);
        Assert.Equal("INV-1", manifest.FirstInvoiceId);
        Assert.Equal("INV-50", manifest.LastInvoiceId);

        var firstInvoice = archive.GetEntry("invoices/INV-1.xml");
        Assert.NotNull(firstInvoice);
        using var invoiceReader = new StreamReader(firstInvoice!.Open(), Encoding.UTF8);
        var xml = await invoiceReader.ReadToEndAsync();
        Assert.StartsWith("<Invoice id=\"1\">", xml);
    }

    [Fact]
    public async Task Sanitizes_unsafe_invoice_ids_in_entry_names()
    {
        var builder = new StreamingZipArchiveBuilder(
            NullLogger<StreamingZipArchiveBuilder>.Instance,
            TimeProvider.System);

        using var memory = new MemoryStream();
        var result = await builder.BuildAsync(
            "unsafe-batch",
            tenantId: null,
            ProduceWithUnsafeIds(),
            memory,
            new BatchLimits(MaxRecordCount: 10, MaxArchiveSizeBytes: long.MaxValue),
            CancellationToken.None);

        Assert.Equal(1, result.InvoiceCount);

        memory.Position = 0;
        using var archive = new ZipArchive(memory, ZipArchiveMode.Read);
        Assert.Null(archive.GetEntry("invoices/../../evil.xml"));
        Assert.NotNull(archive.GetEntry("invoices/.._.._evil.xml"));
    }

    private static async IAsyncEnumerable<Invoice> ProduceWithUnsafeIds()
    {
        yield return new Invoice
        {
            InvoiceId = "../../evil",
            Xml = "<Invoice id=\"evil\"/>",
            CreatedAt = new DateTime(2026, 1, 1, 0, 0, 1, DateTimeKind.Utc)
        };
        await Task.CompletedTask;
    }

    [Fact]
    public async Task Stops_at_max_record_count()
    {
        var builder = new StreamingZipArchiveBuilder(
            NullLogger<StreamingZipArchiveBuilder>.Instance,
            TimeProvider.System);

        var invoices = ProduceInvoices(500);
        using var memory = new MemoryStream();
        var result = await builder.BuildAsync(
            "cap-batch",
            tenantId: null,
            invoices,
            memory,
            new BatchLimits(MaxRecordCount: 10, MaxArchiveSizeBytes: long.MaxValue),
            CancellationToken.None);

        Assert.Equal(10, result.InvoiceCount);
        Assert.True(result.LimitReached);
    }

    private static async IAsyncEnumerable<Invoice> ProduceInvoices(int count)
    {
        var payload = new string('X', 128);
        for (var i = 1; i <= count; i++)
        {
            yield return new Invoice
            {
                InvoiceId = $"INV-{i}",
                Xml = $"<Invoice id=\"{i}\">{payload}</Invoice>",
                CreatedAt = new DateTime(2026, 1, 1, 0, 0, i, DateTimeKind.Utc)
            };
            await Task.Yield();
        }
    }
}
