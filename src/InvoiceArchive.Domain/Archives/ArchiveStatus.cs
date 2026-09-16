namespace InvoiceArchive.Domain.Archives;

public enum ArchiveStatus
{
    Pending = 0,
    Archiving = 1,
    Uploaded = 2,
    Verified = 3,
    Failed = 4,
    Deleted = 5
}
