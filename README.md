# E-Fatura Veri Arşivleme Pipeline

Elasticsearch üzerinde tutulan yüksek hacimli e-fatura verilerini batch'ler halinde
streaming ZIP olarak arşivleyen, arşivi doğrudan S3/MinIO'ya yazan ve Kafka üzerinde
metadata event'i yayınlayan **.NET 8** tabanlı bir Worker Service pipeline'ı.

Bu depo `docs/e_fatura_veri_arsivleme_pipeline_analiz.md` analiz dokümanının
**Yaklaşım 2 — Önerilen** mimarisini uygular: **ZIP binary'si Kafka'dan geçmez, sadece
event/metadata Kafka üzerinden akar.**

---

## İçindekiler

1. [Neden Bu Mimari?](#1-neden-bu-mimari)
2. [Uçtan Uca Akış](#2-uçtan-uca-akış)
3. [Proje Yapısı](#3-proje-yapısı)
4. [Bileşen Bileşen Uygulama Detayı](#4-bileşen-bileşen-uygulama-detayı)
5. [Domain Model & State Machine](#5-domain-model--state-machine)
6. [Kafka Topic ve Event Tasarımı](#6-kafka-topic-ve-event-tasarımı)
7. [S3 Klasör Yapısı ve Object Naming](#7-s3-klasör-yapısı-ve-object-naming)
8. [Idempotency, Retry ve DLQ](#8-idempotency-retry-ve-dlq)
9. [Concurrency ve Backpressure](#9-concurrency-ve-backpressure)
10. [Gözlemlenebilirlik: Metrics + Logs](#10-gözlemlenebilirlik-metrics--logs)
11. [Konfigürasyon](#11-konfigürasyon)
12. [Docker Compose Servisleri](#12-docker-compose-servisleri)
13. [Hızlı Başlangıç](#13-hızlı-başlangıç)
14. [Uçtan Uca Deneme (Seed → Arşiv → Doğrulama)](#14-uçtan-uca-deneme-seed--arşiv--doğrulama)
15. [Testler](#15-testler)
16. [Analiz Dokümanı Eşleşme Tablosu (42 madde)](#16-analiz-dokümanı-eşleşme-tablosu-42-madde)
17. [Netleştirilmesi Gereken Kararlar (MVP Dışı)](#17-netleştirilmesi-gereken-kararlar-mvp-dışı)
18. [Güvenlik](#18-güvenlik)
19. [Ölçeklendirme](#19-ölçeklendirme)

---

## 1. Neden Bu Mimari?

Analiz dokümanı §9–§11 ve §42'de kritik bir mimari karar tanımlanmıştır:

> **ZIP binary'sini Kafka üzerinden mi taşımalıyız, yoksa Kafka sadece event mi taşımalı?**

| Yaklaşım                                          | Sorun                                                                                             |
|---------------------------------------------------|---------------------------------------------------------------------------------------------------|
| **1) ZIP → Kafka → Consumer → S3**                | 500 MB'lık ZIP'ler broker'da disk / network / replication maliyetini patlatır. `message.max.bytes`, `replica.fetch.max.bytes`, `max.request.size` tüm cluster boyunca ayarlanmak zorunda kalır. |
| **2) ZIP → S3 → Kafka Event → Consumer** *(uygulanan)* | Büyük binary storage'a gider, Kafka orchestration/observability için kullanılır. Broker maliyeti düşer, throughput artar. |

Bu depo **Yaklaşım 2**'yi uygular. Karar, aşağıdaki tüm alt bileşenleri şekillendirir:

- Kafka event modeli **metadata-only** (batchId, path, size, sha256, invoiceCount, tenantId, timestamp).
- Kafka partition key = `tenantId` (yoksa `batchId`) → aynı tenant sıralı, farklı tenant'lar paralel işlenebilir.
- ZIP dosyası worker'da **temp file** üzerine yazılır, RAM'de tutulmaz, ardından stream olarak S3'e upload edilir.
- Consumer (`StorageConsumer`) sadece **doğrulama + status güncelleme + DLQ yönetimi** yapar.

---

## 2. Uçtan Uca Akış

```
Elasticsearch  ──PIT + search_after──►  Archive Worker (.NET 8)
                                              │
                                              ├── BatchLimits.IsFull(count, size) → batch kapat
                                              │
                                              ├── Streaming ZIP + manifest.json → temp file
                                              │
                                              ├── SHA-256 hesapla (dosyayı re-read)
                                              │
                                              ├── S3.ObjectExistsAsync? → varsa upload skip (idempotent)
                                              │
                                              ├──► S3 / MinIO
                                              │        Bucket: invoice-archive
                                              │        Key:    {tenant?}/YYYY/MM/DD/batch-{batchId}.zip
                                              │        Metadata: x-amz-meta-sha256, batch-id, invoice-count
                                              │
                                              └──► Kafka: invoice.archive.created
                                                          │  (key = tenantId ?? batchId)
                                                          │  { batchId, path, size, sha256, invoiceCount, ... }
                                                          ▼
                                                  StorageConsumer
                                                          │
                                                          ├── S3 objesini doğrula (HEAD)
                                                          ├── ArchiveBatch.Status = Verified
                                                          ├── Kafka: invoice.archive.completed
                                                          │
                                                          └── (hata) → retry → Kafka: invoice.archive.dlq
                                                                       (headers: x-original-topic/partition/offset/error)
```

State machine (`ArchiveStatus`):

```
Pending → Archiving → Uploaded → Verified → (Deleted)
                          │
                          └─► Failed
```

*Verified → Deleted* geçişi (Elasticsearch'ten silme) analiz §29 gereği **kritik karar**
olarak MVP dışında bırakılmıştır (bkz. bölüm 17).

---

## 3. Proje Yapısı

```
InvoiceArchive.slnx                     .NET 10 SDK format solution
src/
├── InvoiceArchive.Domain               Domain modelleri (framework bağımsız)
│   ├── Archives/ArchiveBatch.cs
│   ├── Archives/ArchiveStatus.cs
│   ├── Archives/ArchiveManifest.cs
│   ├── Archives/BatchLimits.cs
│   └── Invoices/Invoice.cs
│
├── InvoiceArchive.Contracts            Kafka event kontratları
│   ├── Events/ArchiveCreatedEvent.cs
│   ├── Events/ArchiveCompletedEvent.cs
│   ├── Events/ArchiveFailedEvent.cs
│   └── Topics/ArchiveTopics.cs
│
├── InvoiceArchive.Application          Use-case orkestrasyonu (abstract dependencies)
│   ├── Abstractions/                   IInvoiceReader, IZipArchiveBuilder, IArchiveStorage,
│   │                                   IArchiveEventPublisher, IArchiveBatchRepository,
│   │                                   IStorageKeyBuilder, IBatchIdGenerator
│   ├── Configuration/ArchiveOptions.cs
│   └── Services/                       ArchiveService, BatchProcessor,
│                                       BufferedInvoiceStream, DefaultBatchIdGenerator,
│                                       DefaultStorageKeyBuilder
│
├── InvoiceArchive.Infrastructure       Somut adapter'lar
│   ├── Elasticsearch/                  ElasticsearchClientFactory,
│   │                                   ElasticsearchInvoiceReader (PIT + search_after),
│   │                                   ElasticsearchInvoiceDocument
│   ├── Zip/StreamingZipArchiveBuilder.cs
│   ├── Storage/S3ArchiveStorage.cs     Polly retry + ObjectExistsAsync idempotency
│   ├── Kafka/KafkaArchiveEventPublisher.cs
│   ├── Kafka/KafkaArchiveEventConsumer.cs   DLQ + manual commit
│   ├── Persistence/InMemoryArchiveBatchRepository.cs
│   ├── Configuration/                  ElasticsearchOptions, StorageOptions, KafkaOptions
│   └── DependencyInjection.cs
│
├── InvoiceArchive.Worker               BackgroundService (arşivleyici)
│   ├── ArchiveWorker.cs                BatchProcessor'ı çağırır
│   ├── Program.cs                      Serilog + ArchiveOptions binding
│   ├── MetricServerHostedService.cs    Prometheus :9464/metrics
│   ├── Metrics/ArchiveMetrics.cs
│   └── Dockerfile
│
└── InvoiceArchive.StorageConsumer      BackgroundService (doğrulayıcı)
    ├── StorageStatusWorker.cs          Kafka → S3 HEAD → status Verified → completed event
    ├── Program.cs
    └── Dockerfile

tests/
└── InvoiceArchive.Tests                xUnit birim testleri
    ├── StreamingZipArchiveBuilderTests.cs
    ├── ArchiveServiceIdempotencyTests.cs
    ├── BatchProcessorConcurrencyTests.cs
    ├── DefaultStorageKeyBuilderTests.cs
    └── Fakes/                          FakeStorage, FakeEventPublisher

docker-compose.yml                      ES 8.15 + Kafka 3.7 (KRaft) + MinIO + worker + consumer
```

**Katman kuralları** (analiz §20):

- `Domain` hiçbir başka projeye bağlı değildir. Sadece POCO'lar ve state machine.
- `Contracts` sadece `System.*` kullanır — event modelleri publish/consume tarafında paylaşılır.
- `Application` `Domain` + `Contracts`'a bağlıdır; **abstraction'lar** burada tanımlıdır.
- `Infrastructure` `Application`'a bağlıdır ve abstraction'ları implement eder.
- `Worker` ve `StorageConsumer` composition root'lar — DI ve konfigürasyonu birleştirir.

---

## 4. Bileşen Bileşen Uygulama Detayı

### 4.1 `ElasticsearchInvoiceReader` — PIT + search_after (analiz §4)

Analiz §4.1–4.3'te tartışılan üç yöntemden (`from/size`, `scroll`, `PIT + search_after`)
en modern ve tutarlı olan **PIT + search_after** seçilmiştir. Nedenleri:

- Snapshot semantiği (`keep_alive` süresince ES cluster'da tutarlı görünüm).
- Offset pagination'ın maliyeti yok (deep pagination cezası yok).
- `scroll` deprecated eğilimindedir; PIT önerilen yol.

Akış:

1. `OpenPointInTimeAsync(indexName, keepAlive)` çağrılır → `pitId`.
2. `SearchAsync(size=PageSize, sort=[createdAt asc, _id asc], pit=pitId, searchAfter=lastSort)`
3. Dönen `hits`'in son elemanının sort değeri bir sonraki sayfa için `search_after` olur.
4. Hit yoksa döngü biter, `finally` bloğunda `ClosePointInTimeAsync(pitId)`.

Sort iki alanla yapılır (`createdAt`, `_id`) çünkü `createdAt` benzersiz değildir — `_id` tiebreak.

### 4.2 `BatchLimits` — İki Boyutlu Sınır (analiz §5)

```csharp
public sealed record BatchLimits(int MaxRecordCount, long MaxArchiveSizeBytes)
{
    public bool IsFull(int currentCount, long currentSize)
        => currentCount >= MaxRecordCount || currentSize >= MaxArchiveSizeBytes;
}
```

Sadece kayıt sayısına bakmak yetmez (10.000 fatura 50 MB da olabilir, 5 GB da). Bu yüzden
`MaxRecordsPerBatch` (varsayılan 10.000) **VEYA** `MaxArchiveSizeBytes` (varsayılan 500 MiB)
kriterinden **hangisine önce ulaşılırsa** batch kapatılır.

### 4.3 `StreamingZipArchiveBuilder` — Memory-Efficient ZIP (analiz §6–7)

Analiz §6'daki riskli örneği (`MemoryStream.ToArray()`) yapmaz:

- `ZipArchive` mode `Create`, `leaveOpen=true`; hedef stream `FileStream` (temp file).
- Her invoice geldikçe `invoices/{sanitize(id)}.xml` entry'sine yazılır.
- Yazıldıkça `currentCount` ve `currentSize` artar; `BatchLimits.IsFull` true olunca `LimitReached=true` işaretlenip döngü biter.
- Son entry olarak `manifest.json` eklenir.
- Archive `Dispose` edildikten sonra dosya yeniden okunur, **SHA-256** hesaplanır.
- Ne fatura koleksiyonu ne de ZIP baytları RAM'de tutulur.

**Entry name sanitize:** ZIP path traversal riskine karşı `..`, `/`, `\` ve control char'lar `_` ile
değiştirilir.

### 4.4 Manifest (analiz §8.1, §26)

Her ZIP kökünde `manifest.json`:

```json
{
  "batchId": "20260811-000001",
  "createdAt": "2026-08-11T10:20:30Z",
  "invoiceCount": 10000,
  "firstInvoiceId": "1000001",
  "lastInvoiceId": "1010000",
  "tenantId": "tenant-001",
  "compression": "deflate",
  "archiveVersion": 1
}
```

Arşiv integrity'sini tek başına açıklanabilir kılar: ZIP'i alan herhangi bir sistem içeriği
tarayarak batch context'ini kurabilir.

### 4.5 SHA-256 Checksum (analiz §27)

ZIP `Dispose` edildikten sonra dosya baştan okunur ve SHA-256 hesaplanır. Sonuç:

- `ArchiveBatch.Sha256` alanına yazılır (repository).
- S3 upload'da hem `x-amz-meta-sha256` custom metadata olarak eklenir, hem de `IArchiveStorage.UploadAsync` parametresi olarak geçilir.
- `ArchiveCreatedEvent.Sha256` alanında Kafka'ya publish edilir.

Tüketiciler indirilen dosyayı hash'leyip event'teki değerle karşılaştırabilir.

### 4.6 `S3ArchiveStorage` (analiz §13–16, §31)

**AWSSDK.S3** paketi, `ForcePathStyle=true` (MinIO uyumluluğu için).

Sorumluluklar:

- `ObjectExistsAsync(bucket, key)` — `GetObjectMetadataAsync` çağırır, `NotFound` catch edilirse `false` döner.
- `UploadAsync(...)` — `PutObjectRequest` ile stream upload; metadata header'ları eklenir; opsiyonel `ServerSideEncryption=AES256`.
- **Polly ResiliencePipeline**: `MaxRetryAttempts=5`, exponential + jitter, `InitialRetryDelaySeconds=5`. Retry'lenen exception tipleri: `AmazonS3Exception` (Throttling/InternalError/ServiceUnavailable), `HttpRequestException`, `TimeoutException`.
- `Provider=MinIO` veya `Provider=AWS`. AWS için AccessKey/SecretKey boş bırakılırsa **default credential chain** (IAM Role, environment, profile) kullanılır.

### 4.7 `KafkaArchiveEventPublisher` (analiz §10, §18–19)

`Confluent.Kafka` producer, kritik ayarlar:

- `EnableIdempotence=true` — üretici seviyesinde exactly-once-per-partition.
- `Acks=All` — leader + follower confirm.
- `CompressionType=Snappy`.
- `MessageMaxBytes=10 MB` (metadata event'leri KB düzeyinde ama header dahil güvenli üst sınır).
- **Partition key** = `tenantId ?? batchId` (analiz §19). Aynı tenant'ın event'leri sıralı, farklı tenant'lar paralel.
- Message headers: `content-type=application/json`, `event-type=archive.created`.

### 4.8 `KafkaArchiveEventConsumer` — DLQ (analiz §17)

- `EnableAutoCommit=false`, `EnableAutoOffsetStore=false` — hata durumunda offset ilerlemez.
- Handler exception atarsa mesaj **DLQ topic'ine** (`invoice.archive.dlq`) publish edilir.
- DLQ mesajı headers:
  - `x-original-topic`, `x-original-partition`, `x-original-offset`
  - `x-error-message`
  - `x-failed-at` (ISO 8601 UTC)
- DLQ publish başarılı olduktan sonra offset commit edilir → mesaj kaybolmaz, aynı zamanda sonsuz retry döngüsü oluşmaz.

### 4.9 `StorageStatusWorker` (StorageConsumer)

- `invoice.archive.created` topic'ini dinler.
- `ArchiveCreatedEvent.Bucket + .Path` üzerinden `S3.ObjectExistsAsync` doğrulaması yapar.
- Doğrulama başarılıysa:
  - Repository'de `ArchiveStatus = Verified`, `Sha256` set edilir.
  - `invoice.archive.completed` publish edilir.
- Doğrulama başarısızsa exception atılır → consumer altyapısı DLQ'ya taşır.

---

## 5. Domain Model & State Machine

`ArchiveBatch` (analiz §21):

| Alan               | Tip                | Açıklama                                              |
|--------------------|--------------------|-------------------------------------------------------|
| `BatchId`          | `string`           | `yyyyMMdd-nnnnnn` veya `yyyyMMdd-{tenant}-nnnnnn`     |
| `TenantId`         | `string?`          | Multi-tenant senaryoda partition/prefix               |
| `InvoiceCount`     | `int`              | ZIP içindeki gerçek kayıt sayısı                      |
| `SizeInBytes`      | `long`             | ZIP dosya boyutu                                      |
| `FileName`         | `string`           | `batch-{batchId}.zip`                                 |
| `StoragePath`      | `string`           | `{tenant?}/YYYY/MM/DD/batch-{batchId}.zip`            |
| `Sha256`           | `string`           | Hex-encoded SHA-256                                   |
| `FirstInvoiceId`   | `string?`          | Batch'in ilk invoice'ı                                |
| `LastInvoiceId`    | `string?`          | Batch'in son invoice'ı                                |
| `Status`           | `ArchiveStatus`    | State machine (aşağıda)                               |
| `RetryCount`       | `int`              | Kaç kez retry edildi                                  |
| `ErrorMessage`     | `string?`          | Son başarısızlık nedeni                               |
| `CreatedAt`        | `DateTime` (UTC)   | Batch'in oluşturulma anı                              |
| `CompletedAt`      | `DateTime?` (UTC)  | Uploaded/Verified/Failed anı                          |

`ArchiveStatus` state machine (analiz §29):

```
Pending → Archiving → Uploaded → Verified → Deleted
                          │
                          └─► Failed
```

**Kurallar:**

- `Uploaded` = worker ZIP'i S3'e yazdı + Kafka event publish etti.
- `Verified` = StorageConsumer S3 objesini HEAD ile doğruladı.
- `Deleted` = ES kayıtları silindi (MVP dışı — bkz. bölüm 17).
- `Failed` = herhangi bir adımda exception; `ArchiveFailedEvent` publish edilir.

**S3 upload başarılı olmadan ES'ten silme YAPILMAMALIDIR.** State machine bu kuralı zorlar.

---

## 6. Kafka Topic ve Event Tasarımı

| Topic                        | Partitions | Retention | Publisher      | Consumer         |
|------------------------------|------------|-----------|----------------|------------------|
| `invoice.archive.created`    | 3          | default   | Worker         | StorageConsumer  |
| `invoice.archive.completed`  | 3          | default   | StorageConsumer| (downstream)     |
| `invoice.archive.failed`     | 3          | default   | Worker         | (downstream)     |
| `invoice.archive.dlq`        | 3          | default   | Consumer'lar   | (manual replay)  |

**Partition key** (analiz §19): `tenantId` (varsa) sonra `batchId`. Aynı tenant'ın event'leri
sıralı, farklı tenant'lar paralel işlenebilir.

**ArchiveCreatedEvent** payload:

```json
{
  "eventId": "a8d2...",
  "batchId": "20260811-000001",
  "tenantId": "tenant-001",
  "fileName": "batch-20260811-000001.zip",
  "size": 524288000,
  "invoiceCount": 10000,
  "storage": "MinIO",
  "bucket": "invoice-archive",
  "path": "tenant-001/2026/08/11/batch-20260811-000001.zip",
  "sha256": "abc123...",
  "firstInvoiceId": "1000001",
  "lastInvoiceId": "1010000",
  "createdAt": "2026-08-11T10:30:00Z"
}
```

Analiz §18'de tavsiye edilen üç ayrı topic (`created` / `completed` / `failed`) uygulanmıştır.
Başlangıç için tek `invoice.archive` topic'i yeterli olsa da event tiplerini ayırmak
downstream konsumer'ların filter yükünü azaltır.

---

## 7. S3 Klasör Yapısı ve Object Naming

Analiz §13–14 önerileri uygulanmıştır:

**Tenant yoksa:**

```
invoice-archive/
└── 2026/
    └── 08/
        └── 11/
            ├── batch-20260811-000001.zip
            └── batch-20260811-000002.zip
```

**Tenant varsa:**

```
invoice-archive/
└── tenant-001/
    └── 2026/
        └── 08/
            └── 11/
                └── batch-20260811-000001.zip
```

Key builder (`DefaultStorageKeyBuilder`) deterministic:

```csharp
{tenant?}/YYYY/MM/DD/batch-{batchId}.zip
```

Bu deterministic naming §15'teki idempotency stratejisinin temelidir: aynı `batchId`
tekrar işlense de üretilen S3 key değişmez, `ObjectExistsAsync` upload'u skip'ler.

**BatchId formatı** (`DefaultBatchIdGenerator`):

```
yyyyMMdd-nnnnnn                    (tenant yoksa)
yyyyMMdd-{tenant}-nnnnnn           (tenant varsa)
```

`nnnnnn` worker instance içinde monoton artan sıra.

---

## 8. Idempotency, Retry ve DLQ

### 8.1 Idempotency (analiz §15, §30)

Analiz §30'da vurgulandığı gibi ES + Kafka + S3 arasında gerçek exactly-once mümkün değil.
Bu yüzden temel prensip: **at-least-once processing + idempotent operation.**

Uyguladığımız idempotency mekanizmaları:

| Katman            | Mekanizma                                                                                    |
|-------------------|----------------------------------------------------------------------------------------------|
| BatchId           | `yyyyMMdd-{tenant?}-nnnnnn` — deterministic, benzersiz                                       |
| S3 upload         | `ObjectExistsAsync` kontrolü → varsa upload skip                                             |
| Kafka publisher   | `EnableIdempotence=true` — producer seviyesinde partition-per-partition idempotency          |
| Consumer offset   | `EnableAutoCommit=false` — sadece başarılı işleme sonrası commit                             |

Test: `ArchiveServiceIdempotencyTests.Second_upload_is_skipped_when_object_already_exists`
aynı `batchId` iki kez işlenirse `storage.UploadCallCount == 1` olduğunu doğrular.

### 8.2 Retry (analiz §16)

Polly `ResiliencePipeline` — exponential backoff + jitter:

```
Attempt 1  → immediate
Attempt 2  → ~5s   (± jitter)
Attempt 3  → ~10s
Attempt 4  → ~20s
Attempt 5  → ~40s
```

Retry edilebilen exception'lar: `AmazonS3Exception` (5xx/throttle), `HttpRequestException`,
`TimeoutException`. 4xx (AccessDenied, InvalidBucketName) retry edilmez.

### 8.3 DLQ (analiz §17)

Consumer handler'ında yakalanan exception → mesaj `invoice.archive.dlq` topic'ine
publish edilir → offset commit edilir. DLQ mesajı headers:

- `x-original-topic`, `x-original-partition`, `x-original-offset`
- `x-error-message`
- `x-failed-at`

Bu yapıyla:

- Ana topic tıkanmaz (poison message ilerler).
- Kaybolan mesaj olmaz (DLQ tutulur).
- Manual replay için gerekli tüm bağlam header'larda mevcuttur.

---

## 9. Concurrency ve Backpressure

`BatchProcessor` producer/consumer pattern ile çalışır:

```
ES.MoveNextAsync ──► BufferedInvoiceStream ──► Producer (batch materializer)
                                                     │
                                              Channel<ArchiveJob>
                                              (bounded, capacity=N)
                                                     │
                              ┌──────────────────────┼──────────────────────┐
                              ▼                      ▼                      ▼
                         Consumer 1            Consumer 2             Consumer M
                     (ProcessBatchAsync)   (ProcessBatchAsync)    (ProcessBatchAsync)
```

- **Producer:** ES cursor'dan invoice okur, `MaxRecordsPerBatch` **veya** yaklaşık
  `MaxArchiveSizeBytes`'a ulaşana kadar `List<Invoice>` materialize eder, `Channel`'a yazar.
- **Consumers:** `MaxConcurrency` adet paralel tüketici `ArchiveService.ProcessBatchAsync`
  çağırır. `ArchiveService` state'siz + `InMemoryArchiveBatchRepository` `ConcurrentDictionary`
  bazlı → concurrent-safe.

### 9.1 Concurrency (analiz §23)

`ArchiveOptions.MaxConcurrency` (default `1`) kadar consumer paralel çalışır. `1` ile
sequential baseline, `>1` ile N batch aynı anda ZIP + upload aşamasında olur.
Elasticsearch cursor'u tek okuyuculu kalır (producer tek writer) — race yok.

Tuning ipuçları:

- Consumer'lar ZIP + S3 upload CPU/IO bound; storage throughput'una göre 2–8 arası tipik.
- Producer da async — consumer'lar tam paralel dolarsa producer geride kalmaz.
- Multi-instance horizontal scale hâlâ mümkün: her instance farklı `TenantId` veya
  `CreatedBeforeUtc` window işler (partition-by-date / partition-by-tenant).

### 9.2 Backpressure (analiz §24)

`ArchiveOptions.ChannelCapacity` (default `4`) bounded `Channel<ArchiveJob>` kapasitesidir.
`BoundedChannelFullMode.Wait` ile producer, kanal dolduğunda `WriteAsync`'de bekler →
consumer'lar yetişemezse ES okuma da doğal olarak yavaşlar. Böylece:

- Aynı anda bellekte tutulan batch sayısı ≤ `ChannelCapacity + MaxConcurrency`.
- ES hızlı, ZIP yavaşsa RAM patlamaz.
- Consumer hızlı, ES yavaşsa consumer'lar `ReadAllAsync` içinde bekler.

---

## 10. Gözlemlenebilirlik: Metrics + Logs

### 10.1 Prometheus Metrics (analiz §34)

Worker `http://<host>:9464/metrics` endpoint'i açar (`Prometheus.MetricServer`,
HttpListener tabanlı — ASP.NET Core bağımlılığı yok).

| Metric                                   | Tip        | Amacı                                                    |
|------------------------------------------|------------|----------------------------------------------------------|
| `archive_batches_total`                  | Counter    | Başarıyla arşivlenen batch sayısı                        |
| `archive_batches_failed_total`           | Counter    | Başarısız batch sayısı                                   |
| `archive_invoices_total`                 | Counter    | Arşivlenen toplam invoice sayısı                         |
| `archive_zip_size_bytes`                 | Histogram  | ZIP boyutu dağılımı (1 MiB → 4 GiB exponential bucket)   |
| `archive_processing_duration_seconds`    | Histogram  | Batch başına toplam işleme süresi                        |

Örnek `100.000 invoice kaç dakikada arşivleniyor?` sorusuna cevap:

```promql
rate(archive_invoices_total[5m]) * 60
```

### 10.2 Structured Logging (analiz §33)

Serilog, console sink, structured JSON'a yakın compact format. Her batch için:

```
BatchId=20260811-000001 InvoiceCount=10000 ZipSize=523456789 Sha256=abc... Status=Completed
```

logu düşer. Alanlar: `BatchId`, `InvoiceCount`, `SizeBytes`, `Sha256`, `TenantId`,
`ProcessingTime` (implicit via Serilog).

Downstream olarak Elasticsearch/Loki sink eklemek `appsettings.json` içindeki `Serilog`
bölümüne yeni sink tanımlamakla mümkündür (`Serilog.Settings.Configuration` paketi zaten
projelerde var).

---

## 11. Konfigürasyon

Tüm konfigürasyon `src/InvoiceArchive.Worker/appsettings.json` +
`appsettings.Development.json` + environment variable override (double underscore) ile
yönetilir.

Environment variable formatı: `Section__Property=value` (örn.
`Storage__ServiceUrl=http://localhost:9000`).

### 11.1 `Archive` bölümü

| Anahtar                    | Varsayılan  | Açıklama                                                    |
|----------------------------|-------------|-------------------------------------------------------------|
| `IndexName`                | `invoices`  | Kaynak ES index                                             |
| `MaxRecordsPerBatch`       | `10000`     | Batch başına maksimum invoice sayısı                        |
| `MaxArchiveSizeBytes`      | `524288000` | Batch başına maksimum ZIP byte (500 MiB)                    |
| `ElasticsearchPageSize`    | `1000`      | Her ES search çağrısı sayfa boyutu                          |
| `PitKeepAliveMinutes`      | `5`         | PIT snapshot ömrü                                           |
| `MaxConcurrency`           | `1`         | Producer/consumer pattern — kaç consumer paralel ZIP+upload eder |
| `ChannelCapacity`          | `4`         | Bounded `Channel<ArchiveJob>` kapasitesi — backpressure limiti |
| `CreatedBeforeUtc`         | *(null)*    | Sadece bu tarihten önceki invoice'ları arşivle              |
| `TenantId`                 | *(null)*    | Multi-tenant filter + key prefix                            |
| `BucketName`               | `invoice-archive` | S3/MinIO bucket                                       |
| `RunOnceAndExit`           | `true`      | Bir tam pass sonrası exit — cron/K8s Job scheduling için    |

### 11.2 `Elasticsearch` bölümü

| Anahtar                    | Varsayılan                | Açıklama                                  |
|----------------------------|---------------------------|-------------------------------------------|
| `Uri`                      | `http://elasticsearch:9200` | ES endpoint                             |
| `Username` / `Password`    | *(null)*                  | Basic auth                                |
| `ApiKey`                   | *(null)*                  | ES API key auth (Username/Password ile mutually exclusive) |
| `RequestTimeoutSeconds`    | `60`                      | HTTP request timeout                      |

### 11.3 `Storage` bölümü

| Anahtar                     | Varsayılan            | Açıklama                                                 |
|-----------------------------|-----------------------|----------------------------------------------------------|
| `Provider`                  | `MinIO`               | `MinIO` veya `AWS`                                       |
| `ServiceUrl`                | `http://minio:9000`   | MinIO endpoint (AWS için boş bırakılabilir)              |
| `Region`                    | `us-east-1`           | AWS region                                               |
| `AccessKey` / `SecretKey`   | `minioadmin`          | Erişim credentials. AWS'de boş → default credential chain |
| `ForcePathStyle`            | `true`                | MinIO için gerekli, AWS için `false` yapılabilir         |
| `ServerSideEncryption`      | `false`               | `true` → AES256 SSE                                      |
| `MaxRetryAttempts`          | `5`                   | Polly retry sayısı                                       |
| `InitialRetryDelaySeconds`  | `5`                   | Exponential backoff başlangıç gecikmesi                  |

### 11.4 `Kafka` bölümü

| Anahtar                        | Varsayılan                          | Açıklama                             |
|--------------------------------|-------------------------------------|--------------------------------------|
| `BootstrapServers`             | `kafka:9092`                        | Broker adresleri                     |
| `ClientId`                     | `invoice-archive-worker`            | Client identifier                    |
| `ConsumerGroupId`              | `invoice-archive-storage-consumer`  | Consumer group (StorageConsumer)     |
| `ArchiveCreatedTopic`          | `invoice.archive.created`           | Ana event topic                      |
| `ArchiveCompletedTopic`        | `invoice.archive.completed`         | Verified event topic                 |
| `ArchiveFailedTopic`           | `invoice.archive.failed`            | Failure event topic                  |
| `DeadLetterTopic`              | `invoice.archive.dlq`               | DLQ topic                            |
| `SecurityProtocol`             | *(null)*                            | Örn. `SaslPlaintext`, `SaslSsl`      |
| `SaslMechanism`                | *(null)*                            | Örn. `Plain`, `ScramSha256`          |
| `SaslUsername` / `SaslPassword`| *(null)*                            | SASL credentials                     |

---

## 12. Docker Compose Servisleri

| Servis            | Image                                         | Port(lar)         | Amaç                                     |
|-------------------|-----------------------------------------------|-------------------|------------------------------------------|
| `elasticsearch`   | `docker.elastic.co/elasticsearch:8.15.3`      | 9200              | Kaynak veri                              |
| `kafka`           | `bitnami/kafka:3.7` (KRaft mode)              | 9092 (internal) / 29092 (host) | Broker (Zookeeper-less)     |
| `kafka-init`      | `bitnami/kafka:3.7`                           | —                 | Startup'ta 4 topic oluşturur             |
| `minio`           | `minio/minio:RELEASE.2025-01-20T14-49-07Z`    | 9000 (API) / 9001 (Console) | S3 uyumlu storage              |
| `minio-init`      | `minio/mc:RELEASE.2025-01-17T23-25-50Z`       | —                 | Startup'ta `invoice-archive` bucket'ı oluşturur |
| `archive-worker`  | build `src/InvoiceArchive.Worker/Dockerfile`  | 9464              | ES → ZIP → S3 → Kafka                    |
| `storage-consumer`| build `src/InvoiceArchive.StorageConsumer/Dockerfile` | —         | Kafka → S3 verify → Verified/DLQ         |

Kafka listener yapısı:

- `PLAINTEXT://kafka:9092` — Docker network içinden erişim (worker/consumer bunu kullanır).
- `EXTERNAL://localhost:29092` — Host'tan erişim (`kcat`, `kafka-console-consumer` vs.).

Kafka'nın `KAFKA_CFG_MESSAGE_MAX_BYTES=10485760` (10 MB) event'lerin kesinlikle
sığması için — pratikte event'ler KB düzeyinde olur.

---

## 13. Hızlı Başlangıç

### 13.1 Bağımsız test / build (Docker gerektirmez)

```bash
dotnet restore
dotnet build InvoiceArchive.slnx
dotnet test tests/InvoiceArchive.Tests/InvoiceArchive.Tests.csproj
```

### 13.2 Docker Compose ile end-to-end

```bash
# Altyapıyı ayağa kaldır
docker compose up -d elasticsearch kafka kafka-init minio minio-init

# Worker + Consumer'ı build edip başlat
docker compose up --build archive-worker storage-consumer
```

**Erişim noktaları:**

| Servis           | URL / port                            |
|------------------|---------------------------------------|
| Elasticsearch    | http://localhost:9200                 |
| Kafka broker     | localhost:29092 (host) / kafka:9092   |
| MinIO API        | http://localhost:9000                 |
| MinIO Console    | http://localhost:9001 (`minioadmin` / `minioadmin`) |
| Worker /metrics  | http://localhost:9464/metrics         |

---

## 14. Uçtan Uca Deneme (Seed → Arşiv → Doğrulama)

### 14.1 ES'e index + örnek veri seed et

```bash
# Index mapping
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

# Örnek 3 kayıt
curl -X POST "localhost:9200/invoices/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary '
{ "index": {} }
{ "invoiceId": "INV-1", "uuid": "uuid-1", "sender": "A", "receiver": "B", "xml": "<Invoice id=\"1\"/>", "tenantId": "tenant-a", "createdAt": "2026-08-01T10:00:00Z" }
{ "index": {} }
{ "invoiceId": "INV-2", "uuid": "uuid-2", "sender": "A", "receiver": "C", "xml": "<Invoice id=\"2\"/>", "tenantId": "tenant-a", "createdAt": "2026-08-01T10:01:00Z" }
{ "index": {} }
{ "invoiceId": "INV-3", "uuid": "uuid-3", "sender": "A", "receiver": "D", "xml": "<Invoice id=\"3\"/>", "tenantId": "tenant-a", "createdAt": "2026-08-01T10:02:00Z" }
'
```

### 14.2 Worker'ı çalıştır

`RunOnceAndExit=true` olduğu için worker mevcut tüm invoice'ları bir batch'e alır, ZIP'ler,
S3'e yükler ve exit eder.

```bash
docker compose up --build archive-worker
# veya lokal:
dotnet run --project src/InvoiceArchive.Worker
```

### 14.3 MinIO'da doğrula

1. http://localhost:9001 aç → `minioadmin` / `minioadmin` ile giriş.
2. `invoice-archive` bucket'ına gir.
3. `2026/08/01/batch-....zip` altında ZIP dosyasını gör.
4. İndirip aç → `invoices/INV-1.xml`, `invoices/INV-2.xml`, `invoices/INV-3.xml`, `manifest.json`.

### 14.4 Kafka event'ini gör

Docker network dışından:

```bash
docker exec -it invoice-kafka \
  kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic invoice.archive.created \
  --from-beginning
```

Beklenen JSON event: `batchId`, `path`, `size`, `sha256`, `invoiceCount` vb.

### 14.5 Consumer'ın Verified state'ini kontrol et

`storage-consumer` container loglarında:

```
BatchId=... Verified Bucket=invoice-archive Path=... Size=... Sha256=...
```

`invoice.archive.completed` topic'inde `ArchiveCompletedEvent` yayınlanır.

---

## 15. Testler

`tests/InvoiceArchive.Tests` içinde 7 xUnit testi:

| Test                                                                              | Doğruladığı davranış                                                          |
|-----------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `StreamingZipArchiveBuilderTests.Builds_zip_with_manifest_and_counts_correctly`   | 50 invoice → 51 entry (50 XML + manifest.json), SHA-256 dolu, ilk/son id doğru |
| `StreamingZipArchiveBuilderTests.Stops_at_max_record_count`                       | `MaxRecordCount=10` sınırına ulaşınca `LimitReached=true`, invoice sayısı 10  |
| `ArchiveServiceIdempotencyTests.Second_upload_is_skipped_when_object_already_exists` | Aynı batchId iki kere işlense de `storage.UploadCallCount == 1`             |
| `BatchProcessorConcurrencyTests.Processes_all_batches_with_multiple_consumers`    | `MaxConcurrency=4` + `ChannelCapacity=2` → 100 invoice / 10 batch tam işlenir |
| `BatchProcessorConcurrencyTests.Producer_stops_batch_when_approx_size_limit_reached` | Producer, `MaxArchiveSizeBytes` aşımından önce batch'i kapatır             |
| `DefaultStorageKeyBuilderTests.Uses_yyyy_mm_dd_layout_without_tenant`             | Key = `2026/08/11/batch-{id}.zip`                                             |
| `DefaultStorageKeyBuilderTests.Prefixes_tenant_when_provided`                     | Key = `tenant-42/2026/08/11/batch-{id}.zip`                                   |

Çalıştırma:

```bash
dotnet test tests/InvoiceArchive.Tests/InvoiceArchive.Tests.csproj
```

---

## 16. Analiz Dokümanı Eşleşme Tablosu (42 madde)

| §  | Analiz maddesi                                    | Uygulama                                                                                             |
|----|---------------------------------------------------|------------------------------------------------------------------------------------------------------|
| 1  | Projenin amacı                                    | Bu README §1                                                                                         |
| 2  | Batch = fatura gruplaması                         | `BatchProcessor` her iterasyonda bir batch üretir                                                    |
| 3  | Ana bileşenler                                    | Worker → ES reader / BatchProcessor / ZipBuilder / EventPublisher                                    |
| 4  | ES streaming okuma gereği                         | `ElasticsearchInvoiceReader` — `IAsyncEnumerable<Invoice>`                                           |
| 4.1| search_after                                      | Sort by `[createdAt asc, _id asc]` + `search_after` cursor                                           |
| 4.2| PIT + search_after                                | `OpenPointInTimeAsync` + `KeepAlive` + `ClosePointInTimeAsync` (finally)                             |
| 4.3| scroll (alternatif)                               | Kullanılmadı — PIT tercih edildi (modern, deprecated eğilimi yok)                                    |
| 5  | Batch boyutu — record + byte                      | `BatchLimits.IsFull(count, size)` — hangisine önce ulaşılırsa                                        |
| 6  | ZIP memory riski                                  | RAM'de ZIP tutulmuyor — temp file'a stream                                                           |
| 7  | Streaming ZIP                                     | `StreamingZipArchiveBuilder` invoice geldikçe entry yazar                                            |
| 8  | ZIP içerik yapısı                                 | `invoices/{id}.xml` + `manifest.json`                                                                |
| 8.1| Manifest içeriği                                  | `ArchiveManifest` — batchId, createdAt, invoiceCount, compression, archiveVersion                    |
| 9  | Kafka'nın rolü — kritik karar                     | **Yaklaşım 2 uygulandı** — Kafka sadece event/metadata                                               |
| 9.1| ZIP-through-Kafka riski                           | Reddedildi — analiz dokümanındaki gerekçelerle                                                       |
| 10 | Önerilen Kafka event modeli                       | `ArchiveCreatedEvent` — analiz §10'daki JSON yapısıyla birebir uyumlu                                |
| 11 | Önerilen mimari                                   | Bu README §2                                                                                         |
| 12 | Kafka Connect (alternatif)                        | Kullanılmadı — .NET consumer daha uzun vadeli fleksibiliteyi kolaylaştırdı; Connect için door açık   |
| 13 | S3 klasör yapısı                                  | `{tenant?}/YYYY/MM/DD/`                                                                              |
| 14 | Object naming                                     | `batch-{batchId}.zip` — deterministic                                                                |
| 15 | Idempotency                                       | Deterministic key + `ObjectExistsAsync` skip                                                         |
| 16 | Retry — exponential backoff                       | Polly `ResiliencePipeline` — 5 attempt, exp + jitter                                                 |
| 17 | Dead Letter Queue                                 | `KafkaArchiveEventConsumer` fail → `invoice.archive.dlq`                                             |
| 18 | Kafka topic tasarımı                              | Analiz §18'deki 3-topic ayrımı uygulandı + DLQ                                                       |
| 19 | Partition tasarımı                                | Publisher `tenantId ?? batchId` üzerinden partition key belirler                                     |
| 20 | .NET 8 katmanlı yapı                              | Domain / Contracts / Application / Infrastructure / Worker / StorageConsumer                         |
| 21 | Domain model                                      | `ArchiveBatch` + `ArchiveStatus`                                                                     |
| 22 | Archive worker algoritması                        | `BatchProcessor` — producer/consumer, `Channel<ArchiveJob>` + N consumer                             |
| 23 | Concurrency                                       | `MaxConcurrency` opsiyonu — N consumer paralel `ProcessBatchAsync`                                   |
| 24 | Backpressure                                      | Bounded `Channel<ArchiveJob>` (`ChannelCapacity`) + pull-based ES cursor                             |
| 25 | Arşivlenecek veri (XML vs JSON)                   | Şu an XML (`invoice.Xml` alanı) — MVP kararı; metadata için manifest ayrıca var                      |
| 26 | Archive manifest                                  | Uygulandı — first/last invoiceId dahil                                                               |
| 27 | Checksum                                          | SHA-256 — event, S3 metadata, repository, log'lara yazılır                                           |
| 28 | İşlem durumu takibi                               | `IArchiveBatchRepository` (InMemory) — PostgreSQL swap için abstraction hazır                        |
| 29 | ES silme sırası                                   | State machine ile korunuyor; `Verified → Deleted` geçişi MVP dışı (bkz. §17)                         |
| 30 | Exactly-once problemi                             | At-least-once + idempotent operation — tüm bileşenlerde                                              |
| 31 | MinIO                                             | `Storage.Provider=MinIO`, `ForcePathStyle=true`                                                      |
| 32 | Docker Compose                                    | ES + Kafka (KRaft) + MinIO + worker + consumer                                                       |
| 33 | Structured logging                                | Serilog — batch başına BatchId, InvoiceCount, ZipSize, Sha256, Status                                |
| 34 | Metrics                                           | Prometheus — 5 metric (`archive_batches_total` vb.)                                                  |
| 35 | Performans testleri                               | Benchmark scriptleri yok — production'a giderken yapılmalı (MVP dışı)                                |
| 36 | Test senaryoları                                  | Happy path + idempotency + max-record + concurrency + key layout birim testleri (7 test)             |
| 37 | Güvenlik                                          | `ServerSideEncryption=AES256` opsiyon; IAM Role AWS default credential chain (bkz. §18)              |
| 38 | S3 lifecycle policy                               | Uygulama dışı — S3 bucket policy tarafında yapılır                                                   |
| 39 | Önerilen MVP                                      | Bu sürüm MVP + retry/DLQ/metrics/checksum/manifest üstü — analiz §39'un tamamı                       |
| 40 | Production mimarisi                               | Bu README §2 (state tracking için persistent store swap gerekli)                                     |
| 41 | Netleştirilmesi gerekenler                        | Bu README §17                                                                                        |
| 42 | Sonuç ve en kritik mimari karar                   | **Yaklaşım 2** benimsendi                                                                            |

---

## 17. Netleştirilmesi Gereken Kararlar (MVP Dışı)

Analiz §41'de listelenen açık kararlar. MVP'de placeholder olarak bırakıldı — production'a
gitmeden önce ürün + platform ekibi ile netleştirilmeli.

### 17.1 Elasticsearch Silme (§29)

`ArchiveStatus.Deleted` state'i tanımlı ama `Verified → Deleted` geçişi implement edilmedi.

**Neden:** Silme yıkıcı ve geri döndürülemez. En az şu soruların cevabı gerekli:

- Silme hemen mi, yoksa T+N gün sonra mı?
- Silme başarısız olursa retry politikası ne?
- Legal hold / audit gereği bazı invoice'lar hiç silinmemeli mi?
- Silme öncesi S3 objesinin **replicated + versioned** olduğu doğrulanmalı mı?

**Uygulama önerisi:** `EsRetentionPolicy` opsiyonu + ayrı bir `EsRetentionWorker` scheduled job.

### 17.2 Persistent Status Store (§28)

Şu an `InMemoryArchiveBatchRepository`. Worker restart'ta state kaybolur.

**Uygulama önerisi:** `IArchiveBatchRepository`'nin PostgreSQL implementasyonu. Şema
analiz §28'deki `archive_batches` tablosu — id, batch_id, invoice_count, file_size,
storage_path, status, retry_count, created_at, completed_at, error_message.

### 17.3 Arşivlenen Veri Formatı (§25)

Şu an sadece `invoice.Xml` XML olarak ZIP'e giriyor.

**Alternatifler:**

- XML + `metadata.json` (sender, receiver, uuid, createdAt) — audit için faydalı.
- XML only — legal olarak yeterli olabilir.
- JSON (tüm document) — machine-readable ama yasal olarak XML tercih edilir.

### 17.4 AWS S3 Production Credentials

`AccessKey` / `SecretKey` boş bırakılırsa default credential chain (env → IAM Role) kullanılır.
Production'da IAM Role önerilir (§37).

---

## 18. Güvenlik

Analiz §37 gereksinimleri:

| Konu                       | Uygulama                                                                                                        |
|----------------------------|-----------------------------------------------------------------------------------------------------------------|
| Least privilege IAM        | Worker sadece `s3:PutObject`, `s3:GetObject` (idempotency HEAD için), `s3:HeadObject` yetkisine ihtiyaç duyar   |
| Bucket policy              | Bucket-side — `invoice-archive` bucket'ına deny public read policy                                              |
| Encryption in transit      | S3/MinIO endpoint HTTPS olmalı (production); Kafka için `SecurityProtocol=SaslSsl`                              |
| Encryption at rest         | `Storage.ServerSideEncryption=true` → `x-amz-server-side-encryption: AES256`                                    |
| Kafka SASL                 | `Kafka.SecurityProtocol` + `SaslMechanism` + `SaslUsername` / `SaslPassword` — konfigürasyondan                 |
| Secret yönetimi            | Credentials environment variable veya Secret Manager (Azure Key Vault, AWS Secrets Manager) üzerinden inject    |
| E-fatura hassasiyeti       | SSE-KMS + audit logging (S3 access logging) önerilir                                                            |

**Yapılmaması gerekenler:**

- `AccessKey/SecretKey`'i repo'ya commit etmek.
- `s3:*` policy vermek.
- Encryption'sız production endpoint.

---

## 19. Ölçeklendirme

### 19.1 Vertical (tek worker)

- `MaxRecordsPerBatch` ve `MaxArchiveSizeBytes` benchmark'a göre ayarlanır (§35).
- `ElasticsearchPageSize` — ES cluster'ın karşılayabildiği en büyük değer.
- Temp file yazımı için worker'ın disk I/O'sunun (SSD önerilir) yeterli olması gerekir.

### 19.2 Horizontal (multi-instance)

**Partition stratejileri:**

| Strateji                                | Nasıl                                                             | Uyarı                                              |
|-----------------------------------------|-------------------------------------------------------------------|----------------------------------------------------|
| Tenant-based                            | Her worker instance farklı `TenantId` set'i işler                 | Tenant sayısı worker sayısını doğrudan sınırlar    |
| Time-window based                       | Her worker farklı `CreatedBeforeUtc` aralığı işler                | Overlap olmamalı, coordination gerekir             |
| ES index-based                          | Her worker farklı `IndexName` işler                               | Index'lerin tenant/tarih bazlı olması gerekir      |

**Consumer tarafında** partition key = `tenantId` olduğu için StorageConsumer instance
sayısı topic partition sayısına kadar paralelize olur (`invoice.archive.created` = 3 partition).

### 19.3 Storage lifecycle (§38)

Long-term retention için S3 bucket lifecycle policy — worker kodunda değil, bucket
konfigürasyonunda:

```
0-90 gün   → S3 Standard
90-365 gün → S3 Standard-IA
365+ gün   → Glacier / Deep Archive
```

E-fatura yasal saklama süresi (TR mevzuatı için 10 yıl) göz önünde bulundurularak
Glacier Deep Archive maliyet açısından uygun olabilir.

---

## Ek: Analiz Dokümanı

Full teknik analiz için: `docs/e_fatura_veri_arsivleme_pipeline_analiz.md`

Bu depo o analizin uygulama tarafıdır. Analiz dokümanı = **spec**, bu depo = **implementation**.
