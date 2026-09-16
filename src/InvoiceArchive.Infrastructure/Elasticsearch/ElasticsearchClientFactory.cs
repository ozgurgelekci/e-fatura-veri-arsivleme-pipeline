using Elastic.Clients.Elasticsearch;
using Elastic.Transport;
using InvoiceArchive.Infrastructure.Configuration;
using Microsoft.Extensions.Options;

namespace InvoiceArchive.Infrastructure.Elasticsearch;

public sealed class ElasticsearchClientFactory
{
    private readonly Lazy<ElasticsearchClient> _client;

    public ElasticsearchClientFactory(IOptions<ElasticsearchOptions> options)
    {
        var opts = options.Value;

        _client = new Lazy<ElasticsearchClient>(() =>
        {
            var settings = new ElasticsearchClientSettings(new Uri(opts.Uri))
                .RequestTimeout(TimeSpan.FromSeconds(opts.RequestTimeoutSeconds));

            if (!string.IsNullOrEmpty(opts.ApiKey))
            {
                settings = settings.Authentication(new ApiKey(opts.ApiKey));
            }
            else if (!string.IsNullOrEmpty(opts.Username))
            {
                settings = settings.Authentication(new BasicAuthentication(opts.Username, opts.Password ?? string.Empty));
            }

            if (opts.DisableCertificateValidation)
            {
                settings = settings.ServerCertificateValidationCallback((_, _, _, _) => true);
            }

            return new ElasticsearchClient(settings);
        });
    }

    public ElasticsearchClient Client => _client.Value;
}
