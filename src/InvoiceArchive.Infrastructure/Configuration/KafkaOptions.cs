namespace InvoiceArchive.Infrastructure.Configuration;

public sealed class KafkaOptions
{
    public const string SectionName = "Kafka";

    public string BootstrapServers { get; set; } = "localhost:9092";
    public string ClientId { get; set; } = "invoice-archive-worker";
    public string? SaslMechanism { get; set; }
    public string? SecurityProtocol { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
    public string ConsumerGroupId { get; set; } = "invoice-archive-consumer";
    public string ArchiveCreatedTopic { get; set; } = "invoice.archive.created";
    public string ArchiveCompletedTopic { get; set; } = "invoice.archive.completed";
    public string ArchiveFailedTopic { get; set; } = "invoice.archive.failed";
    public string DeadLetterTopic { get; set; } = "invoice.archive.dlq";
    public bool EnableIdempotence { get; set; } = true;
    public int MessageSendMaxRetries { get; set; } = 5;
}
