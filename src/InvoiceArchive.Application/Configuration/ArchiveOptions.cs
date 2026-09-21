using System.ComponentModel.DataAnnotations;

namespace InvoiceArchive.Application.Configuration;

public sealed class ArchiveOptions
{
    public const string SectionName = "Archive";

    [Required, MinLength(1)]
    public string IndexName { get; set; } = "invoices";

    [Range(1, 1_000_000)]
    public int MaxRecordsPerBatch { get; set; } = 10_000;

    [Range(1024, long.MaxValue)]
    public long MaxArchiveSizeBytes { get; set; } = 500L * 1024 * 1024;

    [Range(1, 10_000)]
    public int ElasticsearchPageSize { get; set; } = 1_000;

    [Range(1, 60)]
    public int PitKeepAliveMinutes { get; set; } = 5;

    [Range(1, 64)]
    public int MaxConcurrency { get; set; } = 1;

    [Range(1, 1024)]
    public int ChannelCapacity { get; set; } = 4;

    public DateTime? CreatedBeforeUtc { get; set; }
    public string? TenantId { get; set; }

    [Required, MinLength(1)]
    public string BucketName { get; set; } = "invoice-archive";

    public bool RunOnceAndExit { get; set; } = false;
}
