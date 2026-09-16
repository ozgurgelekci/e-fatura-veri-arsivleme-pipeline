using System.IO.Compression;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Domain.Invoices;
using Microsoft.Extensions.Logging;

namespace InvoiceArchive.Infrastructure.Zip;

public sealed class StreamingZipArchiveBuilder : IZipArchiveBuilder
{
    private static readonly JsonSerializerOptions ManifestJsonOptions = new()
    {
        WriteIndented = true
    };

    private readonly ILogger<StreamingZipArchiveBuilder> _logger;
    private readonly TimeProvider _time;

    public StreamingZipArchiveBuilder(
        ILogger<StreamingZipArchiveBuilder> logger,
        TimeProvider time)
    {
        _logger = logger;
        _time = time;
    }

    public async Task<ZipBuildResult> BuildAsync(
        string batchId,
        string? tenantId,
        IAsyncEnumerable<Invoice> invoices,
        Stream destination,
        BatchLimits limits,
        CancellationToken cancellationToken)
    {
        if (!destination.CanSeek)
        {
            throw new ArgumentException("Destination stream must be seekable.", nameof(destination));
        }

        var startPosition = destination.Position;
        var invoiceCount = 0;
        long approxUncompressed = 0;
        string? firstInvoiceId = null;
        string? lastInvoiceId = null;
        var limitReached = false;

        using (var archive = new ZipArchive(destination, ZipArchiveMode.Create, leaveOpen: true))
        {
            await foreach (var invoice in invoices.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (limits.IsFull(invoiceCount, approxUncompressed))
                {
                    limitReached = true;
                    break;
                }

                var entryName = $"invoices/{Sanitize(invoice.InvoiceId)}.xml";
                var entry = archive.CreateEntry(entryName, CompressionLevel.Optimal);
                await using (var entryStream = entry.Open())
                await using (var writer = new StreamWriter(entryStream, new UTF8Encoding(false)))
                {
                    await writer.WriteAsync(invoice.Xml.AsMemory(), cancellationToken).ConfigureAwait(false);
                }

                invoiceCount++;
                approxUncompressed += Encoding.UTF8.GetByteCount(invoice.Xml);
                firstInvoiceId ??= invoice.InvoiceId;
                lastInvoiceId = invoice.InvoiceId;
            }

            var manifest = new ArchiveManifest
            {
                BatchId = batchId,
                CreatedAt = _time.GetUtcNow().UtcDateTime,
                InvoiceCount = invoiceCount,
                Compression = "deflate",
                ArchiveVersion = 1,
                FirstInvoiceId = firstInvoiceId,
                LastInvoiceId = lastInvoiceId,
                TenantId = tenantId
            };

            var manifestEntry = archive.CreateEntry("manifest.json", CompressionLevel.Optimal);
            await using var manifestStream = manifestEntry.Open();
            await JsonSerializer.SerializeAsync(manifestStream, manifest, ManifestJsonOptions, cancellationToken)
                .ConfigureAwait(false);
        }

        await destination.FlushAsync(cancellationToken).ConfigureAwait(false);

        var endPosition = destination.Position;
        var sizeBytes = endPosition - startPosition;

        destination.Seek(startPosition, SeekOrigin.Begin);
        var sha256 = await ComputeSha256Async(destination, cancellationToken).ConfigureAwait(false);
        destination.Seek(endPosition, SeekOrigin.Begin);

        _logger.LogInformation(
            "BatchId={BatchId} ZIP built. InvoiceCount={InvoiceCount} SizeBytes={SizeBytes} LimitReached={LimitReached}",
            batchId, invoiceCount, sizeBytes, limitReached);

        return new ZipBuildResult(invoiceCount, sizeBytes, sha256, firstInvoiceId, lastInvoiceId, limitReached);
    }

    private static async Task<string> ComputeSha256Async(Stream stream, CancellationToken cancellationToken)
    {
        using var sha = SHA256.Create();
        var buffer = new byte[81920];
        int read;
        while ((read = await stream.ReadAsync(buffer.AsMemory(0, buffer.Length), cancellationToken).ConfigureAwait(false)) > 0)
        {
            sha.TransformBlock(buffer, 0, read, null, 0);
        }
        sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        return Convert.ToHexString(sha.Hash!).ToLowerInvariant();
    }

    private static string Sanitize(string id)
    {
        Span<char> buffer = stackalloc char[id.Length];
        for (var i = 0; i < id.Length; i++)
        {
            var c = id[i];
            buffer[i] = c is (>= '0' and <= '9') or (>= 'a' and <= 'z') or (>= 'A' and <= 'Z') or '-' or '_' or '.'
                ? c
                : '_';
        }
        return new string(buffer);
    }
}
