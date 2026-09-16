namespace InvoiceArchive.Application.Abstractions;

public interface IStorageKeyBuilder
{
    string Build(DateTime timestampUtc, string batchId, string? tenantId);
    string FileName(string batchId);
}
