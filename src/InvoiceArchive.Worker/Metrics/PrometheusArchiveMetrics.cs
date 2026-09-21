using InvoiceArchive.Application.Abstractions;

namespace InvoiceArchive.Worker.Metrics;

public sealed class PrometheusArchiveMetrics : IArchiveMetrics
{
    public void RecordBatchSucceeded(int invoiceCount, long zipSizeBytes, TimeSpan duration)
    {
        ArchiveMetrics.BatchesTotal.Inc();
        ArchiveMetrics.InvoicesTotal.Inc(invoiceCount);
        ArchiveMetrics.ZipSizeBytes.Observe(zipSizeBytes);
        ArchiveMetrics.ProcessingDurationSeconds.Observe(duration.TotalSeconds);
    }

    public void RecordBatchFailed()
    {
        ArchiveMetrics.BatchesFailedTotal.Inc();
    }
}
