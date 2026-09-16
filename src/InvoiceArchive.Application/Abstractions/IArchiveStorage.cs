namespace InvoiceArchive.Application.Abstractions;

public interface IArchiveStorage
{
    Task<bool> ObjectExistsAsync(string bucket, string key, CancellationToken cancellationToken);

    Task UploadAsync(
        string bucket,
        string key,
        Stream content,
        long contentLength,
        string sha256,
        IReadOnlyDictionary<string, string>? metadata,
        CancellationToken cancellationToken);

    string StorageName { get; }
}
