namespace InvoiceArchive.Application.Abstractions;

public interface IBatchIdGenerator
{
    string NewBatchId(DateTime timestampUtc, string? tenantId, long sequence);
}
