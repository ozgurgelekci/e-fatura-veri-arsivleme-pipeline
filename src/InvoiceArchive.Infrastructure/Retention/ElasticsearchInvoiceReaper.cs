using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.QueryDsl;
using InvoiceArchive.Application.Abstractions;
using InvoiceArchive.Domain.Archives;
using InvoiceArchive.Infrastructure.Elasticsearch;
using Microsoft.Extensions.Logging;

namespace InvoiceArchive.Infrastructure.Retention;

public sealed class ElasticsearchInvoiceReaper : IArchivedInvoiceReaper
{
    private readonly ElasticsearchClientFactory _factory;
    private readonly ILogger<ElasticsearchInvoiceReaper> _logger;

    public ElasticsearchInvoiceReaper(
        ElasticsearchClientFactory factory,
        ILogger<ElasticsearchInvoiceReaper> logger)
    {
        _factory = factory;
        _logger = logger;
    }

    public async Task<ReapResult> ReapAsync(
        ArchiveBatch batch,
        string indexName,
        bool dryRun,
        CancellationToken cancellationToken)
    {
        if (!batch.LastInvoiceCreatedAt.HasValue)
        {
            throw new InvalidOperationException(
                $"Batch {batch.BatchId} has no LastInvoiceCreatedAt; refusing to reap.");
        }

        var client = _factory.Client;
        var query = BuildQuery(batch);

        var countRequest = new Elastic.Clients.Elasticsearch.CountRequest(indexName)
        {
            Query = query
        };

        var countResponse = await client.CountAsync(countRequest, cancellationToken).ConfigureAwait(false);
        if (!countResponse.IsValidResponse)
        {
            throw new InvalidOperationException(
                $"Elasticsearch count failed for batch {batch.BatchId}: {countResponse.DebugInformation}");
        }

        var matched = countResponse.Count;

        if (dryRun)
        {
            _logger.LogInformation(
                "[DryRun] BatchId={BatchId} would reap up to {MaxDocs} of {Matched} matched documents from {Index}",
                batch.BatchId, batch.InvoiceCount, matched, indexName);
            return new ReapResult(matched, 0, DryRun: true);
        }

        var deleteRequest = new Elastic.Clients.Elasticsearch.DeleteByQueryRequest(indexName)
        {
            Query = query,
            MaxDocs = batch.InvoiceCount,
            Refresh = true,
            Conflicts = Elastic.Clients.Elasticsearch.Conflicts.Proceed
        };

        var deleteResponse = await client.DeleteByQueryAsync(deleteRequest, cancellationToken).ConfigureAwait(false);
        if (!deleteResponse.IsValidResponse)
        {
            throw new InvalidOperationException(
                $"Elasticsearch delete_by_query failed for batch {batch.BatchId}: {deleteResponse.DebugInformation}");
        }

        var deleted = deleteResponse.Deleted ?? 0;
        _logger.LogInformation(
            "BatchId={BatchId} reaped Deleted={Deleted} Matched={Matched} from {Index}",
            batch.BatchId, deleted, matched, indexName);

        return new ReapResult(matched, deleted, DryRun: false);
    }

    private static Query BuildQuery(ArchiveBatch batch)
    {
        var filters = new List<Query>
        {
            new DateRangeQuery(Field.FromString("createdAt")!)
            {
                Lte = batch.LastInvoiceCreatedAt!.Value.ToString("o")
            }
        };

        if (batch.FirstInvoiceCreatedAt.HasValue)
        {
            filters.Add(new DateRangeQuery(Field.FromString("createdAt")!)
            {
                Gte = batch.FirstInvoiceCreatedAt.Value.ToString("o")
            });
        }

        if (!string.IsNullOrEmpty(batch.TenantId))
        {
            filters.Add(new TermQuery(Field.FromString("tenantId")!) { Value = batch.TenantId });
        }

        return new BoolQuery { Filter = filters };
    }
}
