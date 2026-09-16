using System.Collections.Concurrent;
using InvoiceArchive.Application.Abstractions;

namespace InvoiceArchive.Tests.Fakes;

internal sealed class FakeStorage : IArchiveStorage
{
    public ConcurrentDictionary<string, byte[]> Objects { get; } = new();
    public int UploadCallCount;

    public string StorageName => "Fake";

    public Task<bool> ObjectExistsAsync(string bucket, string key, CancellationToken cancellationToken)
        => Task.FromResult(Objects.ContainsKey($"{bucket}/{key}"));

    public async Task UploadAsync(
        string bucket,
        string key,
        Stream content,
        long contentLength,
        string sha256,
        IReadOnlyDictionary<string, string>? metadata,
        CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref UploadCallCount);
        using var ms = new MemoryStream();
        await content.CopyToAsync(ms, cancellationToken).ConfigureAwait(false);
        Objects[$"{bucket}/{key}"] = ms.ToArray();
    }
}
