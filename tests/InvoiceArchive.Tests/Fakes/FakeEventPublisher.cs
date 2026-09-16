using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Contracts.Events;

namespace InvoiceArchive.Tests.Fakes;

internal sealed class FakeEventPublisher : IArchiveEventPublisher
{
    public List<ArchiveCreatedEvent> Created { get; } = new();
    public List<ArchiveCompletedEvent> Completed { get; } = new();
    public List<ArchiveFailedEvent> Failed { get; } = new();

    public Task PublishCreatedAsync(ArchiveCreatedEvent evt, CancellationToken cancellationToken)
    {
        Created.Add(evt);
        return Task.CompletedTask;
    }

    public Task PublishCompletedAsync(ArchiveCompletedEvent evt, CancellationToken cancellationToken)
    {
        Completed.Add(evt);
        return Task.CompletedTask;
    }

    public Task PublishFailedAsync(ArchiveFailedEvent evt, CancellationToken cancellationToken)
    {
        Failed.Add(evt);
        return Task.CompletedTask;
    }
}
