using InvoiceArchive.Application.Abstractions;

namespace InvoiceArchive.Application.Services;

public sealed class DefaultBatchIdGenerator : IBatchIdGenerator
{
    public string NewBatchId(DateTime timestampUtc, string? tenantId, long sequence)
    {
        var date = timestampUtc.ToString("yyyyMMdd");
        var seq = sequence.ToString("D6");
        return string.IsNullOrWhiteSpace(tenantId)
            ? $"{date}-{seq}"
            : $"{date}-{tenantId}-{seq}";
    }
}
