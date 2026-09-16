namespace InvoiceArchive.Contracts.Topics;

public static class ArchiveTopics
{
    public const string ArchiveCreated = "invoice.archive.created";
    public const string ArchiveCompleted = "invoice.archive.completed";
    public const string ArchiveFailed = "invoice.archive.failed";
    public const string DeadLetter = "invoice.archive.dlq";
}
