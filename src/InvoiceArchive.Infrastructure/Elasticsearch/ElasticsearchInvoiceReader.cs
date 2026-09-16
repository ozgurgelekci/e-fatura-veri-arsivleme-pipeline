using System.Runtime.CompilerServices;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Core.Search;
using Elastic.Clients.Elasticsearch.QueryDsl;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Domain.Invoices;
using Microsoft.Extensions.Logging;

namespace InvoiceArchive.Infrastructure.Elasticsearch;

public sealed class ElasticsearchInvoiceReader : IInvoiceReader
{
    private readonly ElasticsearchClientFactory _factory;
    private readonly ILogger<ElasticsearchInvoiceReader> _logger;

    public ElasticsearchInvoiceReader(
        ElasticsearchClientFactory factory,
        ILogger<ElasticsearchInvoiceReader> logger)
    {
        _factory = factory;
        _logger = logger;
    }

    public async IAsyncEnumerable<Invoice> ReadAsync(
        InvoiceQuery query,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var client = _factory.Client;

        var pitResponse = await client.OpenPointInTimeAsync(
            new OpenPointInTimeRequest(query.IndexName)
            {
                KeepAlive = query.PitKeepAlive
            },
            cancellationToken).ConfigureAwait(false);

        if (!pitResponse.IsValidResponse)
        {
            throw new InvalidOperationException(
                $"Failed to open PIT: {pitResponse.DebugInformation}");
        }

        var pitId = pitResponse.Id;
        _logger.LogInformation("Opened Elasticsearch PIT for index {Index}", query.IndexName);

        try
        {
            IReadOnlyCollection<FieldValue>? searchAfter = null;
            var totalRead = 0L;

            while (!cancellationToken.IsCancellationRequested)
            {
                var searchRequest = new SearchRequest<ElasticsearchInvoiceDocument>
                {
                    Size = query.PageSize,
                    TrackTotalHits = new TrackHits(false),
                    Sort = new List<SortOptions>
                    {
                        SortOptions.Field(new Field("createdAt"), new FieldSort { Order = SortOrder.Asc }),
                        SortOptions.Field(new Field("_id"), new FieldSort { Order = SortOrder.Asc })
                    },
                    Query = BuildQuery(query),
                    Pit = new PointInTimeReference { Id = pitId, KeepAlive = query.PitKeepAlive }
                };

                if (searchAfter is { Count: > 0 })
                {
                    searchRequest.SearchAfter = searchAfter.ToList();
                }

                var response = await client.SearchAsync<ElasticsearchInvoiceDocument>(
                    searchRequest, cancellationToken).ConfigureAwait(false);

                if (!response.IsValidResponse)
                {
                    throw new InvalidOperationException(
                        $"Elasticsearch search failed: {response.DebugInformation}");
                }

                var hits = response.Hits;
                if (hits.Count == 0)
                {
                    _logger.LogInformation(
                        "Elasticsearch stream exhausted. TotalRead={TotalRead}", totalRead);
                    yield break;
                }

                foreach (var hit in hits)
                {
                    if (hit.Source is null)
                    {
                        continue;
                    }
                    var src = hit.Source;
                    yield return new Invoice
                    {
                        InvoiceId = string.IsNullOrEmpty(src.InvoiceId) ? hit.Id ?? string.Empty : src.InvoiceId,
                        Uuid = src.Uuid,
                        Sender = src.Sender,
                        Receiver = src.Receiver,
                        Xml = src.Xml,
                        CreatedAt = src.CreatedAt,
                        TenantId = src.TenantId
                    };
                    totalRead++;
                }

                var lastHit = hits.Last();
                if (lastHit.Sort is null || lastHit.Sort.Count == 0)
                {
                    _logger.LogWarning("Last hit had no sort values, terminating stream.");
                    yield break;
                }
                searchAfter = lastHit.Sort;
            }
        }
        finally
        {
            try
            {
                await client.ClosePointInTimeAsync(
                    new ClosePointInTimeRequest { Id = pitId }, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to close PIT {PitId}", pitId);
            }
        }
    }

    private static Query BuildQuery(InvoiceQuery query)
    {
        var filters = new List<Query>();

        if (query.CreatedBefore.HasValue)
        {
            filters.Add(new DateRangeQuery("createdAt")
            {
                Lt = query.CreatedBefore.Value.ToString("o")
            });
        }

        if (!string.IsNullOrEmpty(query.TenantId))
        {
            filters.Add(new TermQuery("tenantId") { Value = query.TenantId });
        }

        if (filters.Count == 0)
        {
            return new MatchAllQuery();
        }

        return new BoolQuery { Filter = filters };
    }
}
