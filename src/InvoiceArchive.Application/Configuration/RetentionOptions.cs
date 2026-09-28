using System.ComponentModel.DataAnnotations;

namespace InvoiceArchive.Application.Configuration;

public sealed class RetentionOptions
{
    public const string SectionName = "Retention";

    public bool Enabled { get; set; } = false;

    /// <summary>
    /// Elasticsearch index whose invoice documents are eligible for reaping once
    /// their archive batch reaches Verified status and passes the grace period.
    /// </summary>
    [Required, MinLength(1)]
    public string IndexName { get; set; } = "invoices";

    /// <summary>
    /// Grace period after a batch enters Verified status before its invoices become
    /// candidates for deletion. Keep long enough that any downstream consumer can react.
    /// </summary>
    [Range(1, 3650)]
    public int MinAgeAfterVerifiedDays { get; set; } = 30;

    /// <summary>
    /// Interval between retention scans.
    /// </summary>
    [Range(1, 1440)]
    public int PollIntervalMinutes { get; set; } = 60;

    /// <summary>
    /// Maximum batches to inspect per scan (bounds work per cycle).
    /// </summary>
    [Range(1, 10_000)]
    public int BatchesPerRun { get; set; } = 50;

    /// <summary>
    /// Bucket containing the ZIP archive. Consulted when RequireS3ObjectExists=true.
    /// </summary>
    [Required, MinLength(1)]
    public string BucketName { get; set; } = "invoice-archive";

    /// <summary>
    /// If true, verify that the S3/MinIO archive object still exists before reaping
    /// its source invoices. Off only in exceptional recovery scenarios.
    /// </summary>
    public bool RequireS3ObjectExists { get; set; } = true;

    /// <summary>
    /// Default true — the worker logs what it would do and never issues a real delete.
    /// Must be set to false explicitly to enable destructive operations.
    /// </summary>
    public bool DryRun { get; set; } = true;
}
