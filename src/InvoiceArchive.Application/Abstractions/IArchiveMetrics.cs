namespace InvoiceArchive.Application.Abstractions;

public interface IArchiveMetrics
{
    void RecordBatchSucceeded(int invoiceCount, long zipSizeBytes, TimeSpan duration);
    void RecordBatchFailed();
}

public sealed class NullArchiveMetrics : IArchiveMetrics
{
    public static readonly NullArchiveMetrics Instance = new();

    public void RecordBatchSucceeded(int invoiceCount, long zipSizeBytes, TimeSpan duration)
    {
    }

    public void RecordBatchFailed()
    {
    }
}
