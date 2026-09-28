using Amazon.S3;
using Amazon.S3.Model;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Application.Configuration;
using InvoiceArchive.Infrastructure.Configuration;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.Infrastructure.Health;

public sealed class S3HealthCheck : IHealthCheck
{
    private readonly StorageOptions _storage;
    private readonly ArchiveOptions _archive;

    public S3HealthCheck(IOptions<StorageOptions> storage, IOptions<ArchiveOptions> archive)
    {
        _storage = storage.Value;
        _archive = archive.Value;
    }

    public string Name => "storage";

    public async Task<HealthCheckResult> CheckAsync(CancellationToken cancellationToken)
    {
        var config = new AmazonS3Config
        {
            ForcePathStyle = _storage.ForcePathStyle,
            ServiceURL = _storage.ServiceUrl,
            AuthenticationRegion = _storage.Region
        };

        try
        {
            using var client = string.IsNullOrEmpty(_storage.AccessKey)
                ? new AmazonS3Client(config)
                : new AmazonS3Client(_storage.AccessKey, _storage.SecretKey ?? string.Empty, config);

            await client.GetBucketLocationAsync(new GetBucketLocationRequest
            {
                BucketName = _archive.BucketName
            }, cancellationToken).ConfigureAwait(false);

            return HealthCheckResult.Ok();
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Fail(ex.Message);
        }
    }
}
