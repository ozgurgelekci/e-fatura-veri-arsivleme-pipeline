using InvoiceArchive.Application.Abstractions;

namespace InvoiceArchive.Bench.Fakes;

internal sealed class NoopStorage : IArchiveStorage
{
    private readonly TimeSpan _uploadDelay;
    private long _uploadCount;
    private long _uploadedBytes;

    public NoopStorage(TimeSpan uploadDelay)
    {
        _uploadDelay = uploadDelay;
    }

    public long UploadCount => Interlocked.Read(ref _uploadCount);
    public long UploadedBytes => Interlocked.Read(ref _uploadedBytes);

    public string StorageName => "NoopStorage";

    public Task<bool> ObjectExistsAsync(string bucket, string key, CancellationToken cancellationToken)
        => Task.FromResult(false);

    public async Task UploadAsync(
        string bucket,
        string key,
        Stream content,
        long contentLength,
        string sha256,
        IReadOnlyDictionary<string, string>? metadata,
        CancellationToken cancellationToken)
    {
        var buffer = new byte[81920];
        while (await content.ReadAsync(buffer, cancellationToken).ConfigureAwait(false) > 0)
        {
        }

        if (_uploadDelay > TimeSpan.Zero)
        {
            await Task.Delay(_uploadDelay, cancellationToken).ConfigureAwait(false);
        }

        Interlocked.Increment(ref _uploadCount);
        Interlocked.Add(ref _uploadedBytes, contentLength);
    }
}
