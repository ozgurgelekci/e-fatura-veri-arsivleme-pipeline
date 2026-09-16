namespace InvoiceArchive.Application.Configuration;

public sealed class ArchiveOptions
{
    public const string SectionName = "Archive";

    public string IndexName { get; set; } = "invoices";
    public int MaxRecordsPerBatch { get; set; } = 10_000;
    public long MaxArchiveSizeBytes { get; set; } = 500L * 1024 * 1024;
    public int ElasticsearchPageSize { get; set; } = 1_000;
    public int PitKeepAliveMinutes { get; set; } = 5;
    public int MaxConcurrency { get; set; } = 1;
    public int ChannelCapacity { get; set; } = 4;
    public DateTime? CreatedBeforeUtc { get; set; }
    public string? TenantId { get; set; }
    public string BucketName { get; set; } = "invoice-archive";
    public bool RunOnceAndExit { get; set; } = false;
}
