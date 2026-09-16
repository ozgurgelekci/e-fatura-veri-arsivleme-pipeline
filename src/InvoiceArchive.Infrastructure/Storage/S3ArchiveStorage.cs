using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Infrastructure.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;

namespace InvoiceArchive.Infrastructure.Storage;

public sealed class S3ArchiveStorage : IArchiveStorage, IAsyncDisposable
{
    private readonly IAmazonS3 _s3;
    private readonly StorageOptions _options;
    private readonly ILogger<S3ArchiveStorage> _logger;
    private readonly ResiliencePipeline _uploadPipeline;
    private readonly ResiliencePipeline _existsPipeline;

    public S3ArchiveStorage(
        IOptions<StorageOptions> options,
        ILogger<S3ArchiveStorage> logger)
    {
        _options = options.Value;
        _logger = logger;

        var s3Config = new AmazonS3Config
        {
            ForcePathStyle = _options.ForcePathStyle,
            RegionEndpoint = null
        };

        if (!string.IsNullOrEmpty(_options.ServiceUrl))
        {
            s3Config.ServiceURL = _options.ServiceUrl;
        }
        if (!string.IsNullOrEmpty(_options.Region))
        {
            s3Config.AuthenticationRegion = _options.Region;
        }

        AWSCredentials credentials = !string.IsNullOrEmpty(_options.AccessKey)
            ? new BasicAWSCredentials(_options.AccessKey, _options.SecretKey ?? string.Empty)
            : new AnonymousAWSCredentials();

        _s3 = new AmazonS3Client(credentials, s3Config);

        _uploadPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = _options.MaxRetryAttempts,
                Delay = TimeSpan.FromSeconds(_options.InitialRetryDelaySeconds),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(IsTransient),
                OnRetry = args =>
                {
                    _logger.LogWarning(args.Outcome.Exception,
                        "S3 upload retry attempt {Attempt} after {Delay}",
                        args.AttemptNumber + 1, args.RetryDelay);
                    return default;
                }
            })
            .Build();

        _existsPipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = 3,
                Delay = TimeSpan.FromSeconds(1),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder().Handle<Exception>(IsTransient)
            })
            .Build();
    }

    public string StorageName => _options.Provider;

    public async Task<bool> ObjectExistsAsync(string bucket, string key, CancellationToken cancellationToken)
    {
        return await _existsPipeline.ExecuteAsync(async ct =>
        {
            try
            {
                await _s3.GetObjectMetadataAsync(new GetObjectMetadataRequest
                {
                    BucketName = bucket,
                    Key = key
                }, ct).ConfigureAwait(false);
                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == System.Net.HttpStatusCode.NotFound)
            {
                return false;
            }
        }, cancellationToken).ConfigureAwait(false);
    }

    public async Task UploadAsync(
        string bucket,
        string key,
        Stream content,
        long contentLength,
        string sha256,
        IReadOnlyDictionary<string, string>? metadata,
        CancellationToken cancellationToken)
    {
        var startPosition = content.CanSeek ? content.Position : -1;

        await _uploadPipeline.ExecuteAsync(async ct =>
        {
            if (content.CanSeek && startPosition >= 0)
            {
                content.Seek(startPosition, SeekOrigin.Begin);
            }

            var request = new PutObjectRequest
            {
                BucketName = bucket,
                Key = key,
                InputStream = content,
                AutoCloseStream = false,
                ContentType = "application/zip",
                DisablePayloadSigning = _options.Provider.Equals("MinIO", StringComparison.OrdinalIgnoreCase),
                Headers =
                {
                    ContentLength = contentLength
                }
            };

            request.Metadata.Add("x-amz-meta-sha256", sha256);
            if (metadata is not null)
            {
                foreach (var kv in metadata)
                {
                    request.Metadata.Add($"x-amz-meta-{kv.Key}", kv.Value);
                }
            }

            if (_options.ServerSideEncryption)
            {
                request.ServerSideEncryptionMethod = ServerSideEncryptionMethod.AES256;
            }

            var response = await _s3.PutObjectAsync(request, ct).ConfigureAwait(false);
            _logger.LogInformation(
                "Uploaded to {Bucket}/{Key} ETag={ETag} Size={Size}",
                bucket, key, response.ETag, contentLength);
        }, cancellationToken).ConfigureAwait(false);
    }

    private static bool IsTransient(Exception ex)
    {
        if (ex is AmazonS3Exception s3)
        {
            var status = (int)s3.StatusCode;
            return status is 429 or 500 or 502 or 503 or 504
                   || s3.ErrorCode is "RequestTimeout" or "SlowDown" or "InternalError";
        }
        return ex is HttpRequestException or TimeoutException;
    }

    public ValueTask DisposeAsync()
    {
        _s3.Dispose();
        return ValueTask.CompletedTask;
    }
}
