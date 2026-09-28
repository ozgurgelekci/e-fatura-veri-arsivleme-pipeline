using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Contracts.Events;

namespace InvoiceArchive.Bench.Fakes;

internal sealed class NoopEventPublisher : IArchiveEventPublisher
{
    public Task PublishCreatedAsync(ArchiveCreatedEvent evt, CancellationToken cancellationToken)
        => Task.CompletedTask;
    public Task PublishCompletedAsync(ArchiveCompletedEvent evt, CancellationToken cancellationToken)
        => Task.CompletedTask;
    public Task PublishFailedAsync(ArchiveFailedEvent evt, CancellationToken cancellationToken)
        => Task.CompletedTask;
}
