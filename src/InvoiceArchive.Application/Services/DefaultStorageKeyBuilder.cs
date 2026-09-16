using InvoiceArchive.Application.Abstractions;

namespace InvoiceArchive.Application.Services;

public sealed class DefaultStorageKeyBuilder : IStorageKeyBuilder
{
    public string Build(DateTime timestampUtc, string batchId, string? tenantId)
    {
        var year = timestampUtc.ToString("yyyy");
        var month = timestampUtc.ToString("MM");
        var day = timestampUtc.ToString("dd");
        var fileName = FileName(batchId);
        return string.IsNullOrWhiteSpace(tenantId)
            ? $"{year}/{month}/{day}/{fileName}"
            : $"{tenantId}/{year}/{month}/{day}/{fileName}";
    }

    public string FileName(string batchId) => $"batch-{batchId}.zip";
}
