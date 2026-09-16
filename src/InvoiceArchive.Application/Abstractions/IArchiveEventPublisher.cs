using InvoiceArchive.Contracts.Events;

namespace InvoiceArchive.Application.Abstractions;

public interface IArchiveEventPublisher
{
    Task PublishCreatedAsync(ArchiveCreatedEvent evt, CancellationToken cancellationToken);
    Task PublishCompletedAsync(ArchiveCompletedEvent evt, CancellationToken cancellationToken);
    Task PublishFailedAsync(ArchiveFailedEvent evt, CancellationToken cancellationToken);
}
