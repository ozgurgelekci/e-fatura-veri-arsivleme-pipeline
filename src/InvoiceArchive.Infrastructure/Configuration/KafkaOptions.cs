using System.ComponentModel.DataAnnotations;

namespace InvoiceArchive.Infrastructure.Configuration;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    [Required, MinLength(1)]
    public string BootstrapServers { get; set; } = "localhost:9092";

    [Required, MinLength(1)]
    public string ClientId { get; set; } = "invoice-archive-worker";

    public string? SaslMechanism { get; set; }
    public string? SecurityProtocol { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }

    [Required, MinLength(1)]
    public string ConsumerGroupId { get; set; } = "invoice-archive-consumer";

    [Required, MinLength(1)]
    public string ArchiveCreatedTopic { get; set; } = "invoice.archive.created";

    [Required, MinLength(1)]
    public string ArchiveCompletedTopic { get; set; } = "invoice.archive.completed";

    [Required, MinLength(1)]
    public string ArchiveFailedTopic { get; set; } = "invoice.archive.failed";

    [Required, MinLength(1)]
    public string DeadLetterTopic { get; set; } = "invoice.archive.dlq";

    public bool EnableIdempotence { get; set; } = true;

    [Range(0, 100)]
    public int MessageSendMaxRetries { get; set; } = 5;
}
