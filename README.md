# E-Fatura Veri Arşivleme Pipeline

Elasticsearch üzerinde tutulan e-fatura kayıtlarını batch'ler halinde streaming ZIP olarak arşivleyen, arşivi doğrudan S3/MinIO'ya yazan ve Kafka üzerinde metadata event'i yayınlayan .NET 8 tabanlı bir pipeline.

Bu depo `docs/e_fatura_veri_arsivleme_pipeline_analiz.md` doküman analizindeki **Yaklaşım 2 – Önerilen** mimariyi uygular: ZIP binary'si Kafka'dan geçmez, sadece event/metadata Kafka üzerinden akar.

## Mimari

```
Elasticsearch  ──PIT + search_after──►  Archive Worker (.NET 8)
                                              │
                                              ├── Streaming ZIP + manifest.json + SHA256
                                              │
                                              ├──► S3 / MinIO (invoice-archive/YYYY/MM/DD/…)
                                              │
                                              └──► Kafka: invoice.archive.created
                                                          │
                                                          ▼
                                                  StorageConsumer
                                                          │
                                                          ├── S3 objesini doğrula
                                                          ├── Batch status = Verified
                                                          └── Kafka: invoice.archive.completed  |  .dlq
```

## Proje Yapısı

```
InvoiceArchive.slnx
src/
├── InvoiceArchive.Domain           # ArchiveBatch, Invoice, ArchiveManifest, BatchLimits, ArchiveStatus
├── InvoiceArchive.Contracts        # ArchiveCreated/Completed/FailedEvent, ArchiveTopics
├── InvoiceArchive.Application      # Abstractions, ArchiveService, BatchProcessor, ArchiveOptions
├── InvoiceArchive.Infrastructure   # ES reader, streaming ZIP, S3/MinIO storage, Kafka producer/consumer
├── InvoiceArchive.Worker           # BackgroundService + Prometheus /metrics endpoint
└── InvoiceArchive.StorageConsumer  # Kafka -> S3 doğrulama + status güncelleme + DLQ
tests/
└── InvoiceArchive.Tests            # xUnit birim testleri
docker-compose.yml                  # Elasticsearch, Kafka (KRaft), MinIO, worker, consumer
```

## Analiz dokümanı ile eşleşen kararlar

| Analiz maddesi                         | Uygulama                                                                                       |
|----------------------------------------|------------------------------------------------------------------------------------------------|
| 4. ES streaming okuma                  | `ElasticsearchInvoiceReader` PIT + `search_after` ile sayfalar                                 |
| 5. Batch boyutu (kayıt + byte)         | `BatchLimits.IsFull(count, size)` — hangisine önce ulaşılırsa                                  |
| 7. Streaming ZIP                       | `StreamingZipArchiveBuilder` invoice geldikçe temp file'a yazar, RAM'de tutmaz                 |
| 8/26. Manifest                         | Her ZIP içinde `manifest.json` (batchId, invoiceCount, first/last invoiceId, tenant, tarih)    |
| 10. Kafka event modeli                 | ZIP S3'e, event Kafka'ya (`invoice.archive.created`) — binary Kafka'dan geçmez                 |
| 13/14. S3 klasör yapısı & object name  | `{tenant?}/YYYY/MM/DD/batch-{batchId}.zip` deterministic                                       |
| 15. Idempotency                        | Yükleme öncesi `ObjectExistsAsync` kontrolü + benzersiz `batchId`                              |
| 16. Retry (exponential backoff)        | Polly `ResiliencePipeline` — 5 deneme, exponential+jitter                                      |
| 17. Dead Letter Queue                  | `KafkaArchiveEventConsumer` başarısız mesajı `invoice.archive.dlq` topic'ine yollar            |
| 19. Partition key                      | Kafka publisher `tenantId` (yoksa `batchId`) üzerinden partition seçer                         |
| 20. .NET 8 katmanlı yapı               | Domain / Contracts / Application / Infrastructure / Worker / StorageConsumer                   |
| 21. Domain model                       | `ArchiveBatch` + `ArchiveStatus` state machine (Pending→Archiving→Uploaded→Verified→Deleted)   |
| 24. Backpressure                       | Streaming zaten pull-based; sequential worker upstream tıkanması olmaz                         |
| 27. SHA-256 checksum                   | ZIP kapandıktan sonra hesaplanır; event ve S3 metadata'sına yazılır                            |
| 28. İşlem takibi                       | `IArchiveBatchRepository` (InMemory implementasyon; PostgreSQL için ileride swap edilebilir)   |
| 29. ES silme sırası                    | Sadece `Uploaded/Verified` sonrasına bırakılmıştır (silme adımı henüz açık — kritik karar)     |
| 31/32. MinIO + docker-compose          | `docker-compose.yml` ES + KRaft Kafka + MinIO + worker + consumer                              |
| 33/34. Metrics + structured logging    | Prometheus counters/histograms `:9464/metrics`, Serilog console structured                     |

## Hızlı başlangıç

### 1) Bağımsız test / build (Docker gerektirmez)
```bash
dotnet restore
dotnet build InvoiceArchive.slnx
dotnet test tests/InvoiceArchive.Tests/InvoiceArchive.Tests.csproj
```

### 2) Docker Compose ile end-to-end (Docker Desktop gerekir)
```bash
docker compose up -d elasticsearch kafka kafka-init minio minio-init
docker compose up --build archive-worker storage-consumer
```
Servisler:

| Servis           | URL / port                            |
|------------------|---------------------------------------|
| Elasticsearch    | http://localhost:9200                 |
| Kafka broker     | localhost:29092 (host) / kafka:9092   |
| MinIO API        | http://localhost:9000                 |
| MinIO Console    | http://localhost:9001 (`minioadmin`)  |
| Worker /metrics  | http://localhost:9464/metrics         |

### 3) Örnek e-fatura verisi seed etmek
```bash
curl -X PUT "localhost:9200/invoices" -H 'Content-Type: application/json' -d '
{
  "mappings": {
    "properties": {
      "invoiceId":  { "type": "keyword" },
      "uuid":       { "type": "keyword" },
      "sender":     { "type": "keyword" },
      "receiver":   { "type": "keyword" },
      "xml":        { "type": "text" },
      "tenantId":   { "type": "keyword" },
      "createdAt":  { "type": "date" }
    }
  }
}'
```

## Konfigürasyon

`src/InvoiceArchive.Worker/appsettings.json` (aynı bölümler `appsettings.Development.json` ve environment variable'lar ile override edilebilir — `__` çift alt çizgi kullanılır, örn. `Storage__ServiceUrl=http://localhost:9000`).

| Bölüm             | Ana anahtarlar                                                                                     |
|-------------------|----------------------------------------------------------------------------------------------------|
| `Archive`         | `IndexName`, `MaxRecordsPerBatch`, `MaxArchiveSizeBytes`, `BucketName`, `TenantId`, `RunOnceAndExit` |
| `Elasticsearch`   | `Uri`, `Username`/`Password` veya `ApiKey`, `RequestTimeoutSeconds`                                |
| `Storage`         | `Provider` (MinIO/AWS), `ServiceUrl`, `Region`, `AccessKey`, `SecretKey`, `ServerSideEncryption`   |
| `Kafka`           | `BootstrapServers`, topic isimleri, SASL (`SecurityProtocol`, `SaslMechanism`, user/pass)          |

Batch tamamlanma kriteri: `MaxRecordsPerBatch` VE `MaxArchiveSizeBytes` — hangisine önce ulaşılırsa ZIP kapanır.

## Test edilen senaryolar

`InvoiceArchive.Tests` şunları doğrular:
- Streaming ZIP builder doğru sayıda invoice + `manifest.json` üretir, deterministic hash döner
- `MaxRecordCount` sınırına ulaşıldığında batch kapanır
- Aynı `batchId` iki kez işlense de S3'e yalnızca bir defa yüklenir (idempotency)
- Object key deterministic: `{tenant?}/YYYY/MM/DD/batch-{batchId}.zip`

## Netleştirilmesi gereken kararlar (MVP dışı)

Analiz dokümanı §41'de sorulan kritik sorular — MVP'de placeholder ile bırakıldı:
- **Elasticsearch silme** — `ArchiveStatus.Deleted` state'i mevcut, ancak `Verified → Deleted` geçişi henüz implement edilmedi. S3 doğrulaması `StorageConsumer` içinde yapılıyor; silme adımı ürün kararına bağlı.
- **Persistent status store** — Repository şu an in-memory. PostgreSQL implementasyonu için `IArchiveBatchRepository` interface'i hazır.
- **Concurrency** — `MaxConcurrency=1` (dokümanın MVP önerisi). Paralellik için worker'ı `TenantId` veya tarih partition'ına göre birden çok instance olarak deploy etmek önerilir.
- **AWS S3 üretim** — `Storage.Provider=AWS` + `AccessKey/SecretKey` boş bırakılırsa default credential zinciri (IAM Role vb.) kullanılır.

## Görüntüleme / gözlemlenebilirlik

Prometheus metric'leri worker'da:

```
archive_batches_total
archive_batches_failed_total
archive_invoices_total
archive_zip_size_bytes           (histogram, 1 MiB'den 4 GiB'e üstel bucket)
archive_processing_duration_seconds
```

Structured logging (Serilog console): `BatchId`, `InvoiceCount`, `ZipSize`, `Sha256`, `ProcessingTime` alanları her batch için loglanır.
