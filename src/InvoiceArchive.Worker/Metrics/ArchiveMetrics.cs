using Prometheus;

namespace InvoiceArchive.Worker.Metrics;

public static class ArchiveMetrics
{
    public static readonly Counter BatchesTotal = Prometheus.Metrics.CreateCounter(
        "archive_batches_total",
        "Total number of archive batches processed.");

    public static readonly Counter BatchesFailedTotal = Prometheus.Metrics.CreateCounter(
        "archive_batches_failed_total",
        "Total number of archive batches that failed.");

    public static readonly Counter InvoicesTotal = Prometheus.Metrics.CreateCounter(
        "archive_invoices_total",
        "Total number of invoices archived.");

    public static readonly Histogram ZipSizeBytes = Prometheus.Metrics.CreateHistogram(
        "archive_zip_size_bytes",
        "Size distribution of created ZIP archives in bytes.",
        new HistogramConfiguration
        {
            Buckets = Histogram.ExponentialBuckets(1_048_576, 2, 12)
        });

    public static readonly Histogram ProcessingDurationSeconds = Prometheus.Metrics.CreateHistogram(
        "archive_processing_duration_seconds",
        "Time taken to build and upload a batch.",
        new HistogramConfiguration
        {
            Buckets = Histogram.ExponentialBuckets(0.5, 2, 10)
        });
}
