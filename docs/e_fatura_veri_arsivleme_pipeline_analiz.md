# E-Fatura Veri Arşivleme Pipeline – Teknik Analiz

## 1. Projenin Amacı

Elasticsearch üzerinde tutulan e-fatura verilerinin belirli kriterlere göre toplu olarak alınması, batch'ler halinde ZIP arşivlerine dönüştürülmesi ve oluşturulan arşivlerin Kafka üzerinden AWS S3 veya lokal geliştirme ortamında MinIO üzerinde saklanması amaçlanmaktadır.

### Genel Mimari

```text
             ┌─────────────────────┐
             │    Elasticsearch     │
             │                     │
             │  E-Fatura Verileri  │
             └──────────┬──────────┘
                        │
                        │ Search / Scroll
                        ▼
             ┌─────────────────────┐
             │  Archive Worker     │
             │      (.NET 8)       │
             │                     │
             │ Batch oluşturma     │
             │ ZIP oluşturma       │
             └──────────┬──────────┘
                        │
                        │ ZIP Archive
                        ▼
             ┌─────────────────────┐
             │    Apache Kafka     │
             │                     │
             │  Archive Events     │
             └──────────┬──────────┘
                        │
                        ▼
             ┌─────────────────────┐
             │ Kafka Consumer /    │
             │ Kafka Connect       │
             └──────────┬──────────┘
                        │
                        ▼
        ┌───────────────────────────────┐
        │       AWS S3 / MinIO          │
        │                               │
        │     ZIP Arşivleri             │
        └───────────────────────────────┘
```

---

## 2. "Batch Olarak Ziplemek" Ne Demek?

Batch, Elasticsearch'teki çok sayıdaki e-fatura kaydının belirli büyüklükte gruplara ayrılması anlamına gelir.

Örneğin Elasticsearch'te 10.000.000 invoice olduğunu düşünelim:

```text
Batch 1  → 10.000 invoice → archive-000001.zip
Batch 2  → 10.000 invoice → archive-000002.zip
Batch 3  → 10.000 invoice → archive-000003.zip
...
Batch 1000 → 10.000 invoice → archive-001000.zip
```

Her ZIP içerisinde birden fazla e-fatura bulunur:

```text
batch-000001.zip
│
├── invoice-100001.xml
├── invoice-100002.xml
├── invoice-100003.xml
├── ...
└── invoice-110000.xml
```

Buradaki 10.000 değeri örnektir. Gerçek batch size performans testleri sonucunda belirlenmelidir.

---

## 3. Sistemin Ana Bileşenleri

Sistem temel olarak aşağıdaki bileşenlerden oluşabilir:

```text
Archive Worker
    │
    ├── ElasticsearchService
    │
    ├── BatchProcessor
    │
    ├── ZipArchiveService
    │
    └── KafkaProducer
```

### Archive Worker

.NET 8 Worker Service olarak çalışabilir.

Sorumlulukları:

- Elasticsearch'ten arşivlenecek kayıtları bulmak
- Verileri batch'lemek
- ZIP oluşturmak
- ZIP metadata'sını oluşturmak
- Kafka'ya event göndermek
- Başarılı/başarısız batch'leri takip etmek

---

# 4. Elasticsearch'ten Veri Okuma

Milyonlarca kaydı tek seferde almak doğru değildir.

Örneğin:

```csharp
var invoices = await elasticClient.SearchAsync<Invoice>(x =>
    x.Size(1000000)
);
```

gibi bir yaklaşım ciddi memory problemi oluşturabilir.

Örneğin:

```text
1.000.000 invoice
×
ortalama 20 KB
=
~20 GB
```

gibi bir dataset oluşabilir.

Bu nedenle Elasticsearch'ten veriler batch/stream mantığında okunmalıdır.

---

## 4.1 Search After

Büyük datasetlerde `search_after` kullanılabilir.

Örnek:

```text
Batch 1
    ↓
10.000 records

last sort value
    ↓

Batch 2
    ↓
10.000 records

last sort value
    ↓

Batch 3
```

Bu sayede klasik offset pagination'ın maliyetlerinden kaçınılabilir.

---

## 4.2 PIT + Search After

Daha tutarlı bir snapshot üzerinde çalışmak için:

```text
Point In Time
+
search_after
```

kullanılabilir.

Akış:

```text
PIT oluştur
      ↓
10.000 kayıt oku
      ↓
ZIP oluştur
      ↓
Kafka'ya gönder
      ↓
search_after
      ↓
10.000 kayıt daha
```

Büyük arşivleme operasyonlarında bu yaklaşım değerlendirilebilir.

---

## 4.3 Scroll

Scroll da büyük datasetlerin batch olarak okunması için kullanılabilir.

Örneğin:

```text
Scroll
   ↓
10.000
   ↓
10.000
   ↓
10.000
   ↓
...
```

Ancak Elasticsearch sürümüne ve kullanım senaryosuna göre `search_after + PIT` daha uygun olabilir.

---

# 5. Batch Boyutu

Batch size sistem performansını ciddi şekilde etkiler.

Test edilebilecek değerler:

```text
1.000
5.000
10.000
25.000
50.000
```

Ancak yalnızca kayıt sayısına göre batch belirlemek yeterli değildir.

Örneğin:

```text
10.000 invoice → 500 MB ZIP
```

olabileceği gibi:

```text
10.000 invoice → 5 GB ZIP
```

de olabilir.

Bu nedenle daha sağlıklı yaklaşım:

```text
MAX_RECORD_COUNT = 10.000

VEYA

MAX_ARCHIVE_SIZE = 500 MB
```

şeklinde olabilir.

Hangisine önce ulaşılırsa batch kapatılır.

---

# 6. ZIP Oluşturma

ZIP oluştururken en önemli konu memory kullanımını kontrol etmektir.

Aşağıdaki yaklaşım büyük batch'lerde risklidir:

```csharp
using var memoryStream = new MemoryStream();

CreateZip(invoices, memoryStream);

await kafka.SendAsync(memoryStream.ToArray());
```

Çünkü aynı veri birden fazla memory alanında tutulabilir:

```text
Elasticsearch
      ↓
Memory
      ↓
ZIP
      ↓
Memory
      ↓
Kafka
```

Büyük batch'lerde:

```text
OutOfMemoryException
```

riski ortaya çıkabilir.

---

# 7. Streaming ZIP

Daha doğru yaklaşım ZIP'i streaming olarak oluşturmaktır.

```text
Elasticsearch
      ↓
Invoice
      ↓
ZipArchiveEntry
      ↓
ZIP Stream
```

Her invoice geldiğinde ZIP içerisine yazılır:

```text
batch-001.zip

invoice-001.xml → write
invoice-002.xml → write
invoice-003.xml → write
...
```

Böylece bütün invoice'ları RAM'de tutmak gerekmez.

---

# 8. ZIP İçeriği

ZIP içerisindeki dosya yapısı standartlaştırılmalıdır.

Örneğin:

```text
batch-000001.zip
│
├── invoices/
│   ├── 10000001.xml
│   ├── 10000002.xml
│   ├── 10000003.xml
│   └── ...
│
└── manifest.json
```

---

## 8.1 Manifest

ZIP içerisinde `manifest.json` bulunması faydalı olacaktır.

Örnek:

```json
{
  "batchId": "20260811-000001",
  "createdAt": "2026-08-11T10:30:00Z",
  "invoiceCount": 10000,
  "compression": "deflate",
  "archiveVersion": 1
}
```

Böylece arşivin içeriği hakkında hızlıca bilgi edinilebilir.

---

# 9. Kafka'nın Rolü

Burada kritik bir mimari karar bulunmaktadır.

Kafka üzerinden:

```text
ZIP binary data
```

taşımak ile:

```text
ZIP metadata + location
```

taşımak arasında ciddi fark vardır.

---

## 9.1 ZIP'i Kafka Üzerinden Taşımak

Akış:

```text
ZIP
 ↓
Kafka
 ↓
Consumer
 ↓
S3
```

Ancak ZIP dosyası örneğin 500 MB ise Kafka message size problemi ortaya çıkar.

Kafka tarafında:

```text
message.max.bytes
replica.fetch.max.bytes
max.request.size
```

gibi ayarların tamamının düşünülmesi gerekir.

Ayrıca büyük binary dosyaların Kafka broker'larında tutulması ciddi disk ve network maliyeti oluşturabilir.

---

# 10. Önerilen Kafka Modeli

Büyük ZIP dosyalarının kendisini Kafka üzerinden geçirmek yerine Kafka'nın archive event taşıması daha sağlıklı olabilir.

Örnek event:

```json
{
  "eventId": "a8d2...",
  "batchId": "20260811-000001",
  "fileName": "batch-000001.zip",
  "size": 524288000,
  "invoiceCount": 10000,
  "storage": "s3",
  "bucket": "invoice-archive",
  "path": "2026/08/11/batch-000001.zip"
}
```

Bu durumda Kafka:

```text
Data transport
```

yerine:

```text
Event transport
```

olarak kullanılır.

---

# 11. Önerilen Mimari

Daha ölçeklenebilir mimari:

```text
Elasticsearch
      ↓
.NET Archive Worker
      ↓
ZIP oluştur
      ↓
S3 / MinIO
      ↓
Kafka Event
      ↓
Consumer
      ↓
Metadata / Status
```

Burada büyük binary dosya storage'a yazılır, Kafka ise bu işlemin event'ini taşır.

Bu yaklaşım büyük dosyaların Kafka üzerinden taşınmasının oluşturacağı:

- Broker disk kullanımı
- Network yükü
- Message size problemleri
- Replication maliyeti

gibi problemleri azaltır.

---

# 12. Kafka Connect Kullanımı

Kafka Connect ile storage tarafındaki işlemler uygulama kodundan ayrılabilir.

Örneğin:

```text
Kafka
  │
  ▼
Kafka Connect
  │
  ▼
S3 Sink Connector
  │
  ▼
AWS S3
```

Ancak Kafka Connect'in beklediği Kafka data modeli ile ZIP binary modeli uyumlu tasarlanmalıdır.

Bu nedenle proje başlangıcında şu karar netleştirilmelidir:

> ZIP binary Kafka message olarak mı gönderilecek, yoksa Kafka yalnızca archive metadata/event mi taşıyacak?

---

# 13. S3 Klasör Yapısı

S3 üzerinde dosyaların düzenli tutulması gerekir.

Örneğin:

```text
invoice-archive/
│
├── 2026/
│   ├── 08/
│   │   ├── 11/
│   │   │   ├── batch-000001.zip
│   │   │   ├── batch-000002.zip
│   │   │   └── batch-000003.zip
│   │   │
│   │   └── 12/
│   │       └── ...
```

Tenant/company bazlı yapı da kullanılabilir:

```text
invoice-archive/
│
├── tenant-001/
│   └── 2026/
│       └── 08/
│           └── 11/
│
└── tenant-002/
    └── 2026/
        └── 08/
            └── 11/
```

---

# 14. Object Naming

Dosya isimleri deterministic olmalıdır.

Örneğin:

```text
2026/08/11/
batch-20260811-000001.zip
```

veya:

```text
2026/08/11/
tenant-123_batch-000001.zip
```

Böylece aynı batch tekrar işlendiğinde duplicate kontrolü yapılabilir.

---

# 15. Idempotency

Dağıtık sistemlerde aynı batch'in birden fazla kez işlenmesi mümkündür.

Örneğin:

```text
Batch 123
   ↓
ZIP oluşturuldu
   ↓
Kafka gönderildi
   ↓
Consumer çöktü
```

Consumer tekrar başladığında:

```text
Batch 123
```

tekrar işlenebilir.

Bu nedenle her batch benzersiz bir `batchId` taşımalıdır.

Örneğin:

```text
20260811-tenant123-000001
```

S3 object key:

```text
2026/08/11/tenant123/batch-000001.zip
```

olabilir.

Consumer:

```text
if object exists
    skip
else
    upload
```

mantığıyla çalışabilir.

---

# 16. Retry Mekanizması

S3 upload sırasında:

```text
Network timeout
503
429
AccessDenied
```

gibi hatalar oluşabilir.

Retry mekanizması bulunmalıdır.

Örneğin exponential backoff:

```text
1. deneme
   ↓
5 saniye
   ↓
2. deneme
   ↓
10 saniye
   ↓
3. deneme
   ↓
20 saniye
```

Örnek:

```text
5s
10s
20s
40s
80s
```

---

# 17. Dead Letter Queue

Başarısız mesajların kaybolmaması için DLQ kullanılabilir.

Topic'ler:

```text
invoice.archive
invoice.archive.dlq
```

Akış:

```text
Kafka
  │
  ▼
Consumer
  │
  ├── SUCCESS → S3
  │
  └── FAIL
       ↓
     Retry
       ↓
     FAIL
       ↓
     DLQ
```

---

# 18. Kafka Topic Tasarımı

Başlangıç için:

```text
invoice.archive
```

ve:

```text
invoice.archive.dlq
```

yeterli olabilir.

Mesaj:

```json
{
  "batchId": "20260811-000001",
  "fileName": "batch-000001.zip",
  "invoiceCount": 10000
}
```

İleride:

```text
invoice.archive.created
invoice.archive.completed
invoice.archive.failed
```

gibi event'ler ayrıştırılabilir.

---

# 19. Kafka Partition Tasarımı

Partition key olarak `tenantId` kullanılabilir.

```text
tenant-1 → partition 0
tenant-2 → partition 1
tenant-3 → partition 2
```

Alternatif olarak:

```text
batchId
```

kullanılabilir.

Eğer sıralama gerekmiyorsa batch'lerin paralel işlenmesi sağlanabilir.

---

# 20. .NET 8 Proje Yapısı

Önerilen yapı:

```text
InvoiceArchive.sln

src/
│
├── InvoiceArchive.Worker
├── InvoiceArchive.Application
├── InvoiceArchive.Domain
├── InvoiceArchive.Infrastructure
└── InvoiceArchive.Contracts
```

### Worker

`BackgroundService` olarak çalışabilir.

### Application

İş kuralları:

```text
ArchiveInvoicesHandler
BatchProcessor
ArchiveService
```

### Infrastructure

```text
Elasticsearch
Kafka
S3
MinIO
ZIP
```

implementasyonları.

### Contracts

Kafka event modelleri:

```text
ArchiveCreatedEvent
ArchiveCompletedEvent
ArchiveFailedEvent
```

---

# 21. Domain Model

Örneğin:

```csharp
public sealed class ArchiveBatch
{
    public Guid BatchId { get; set; }

    public int InvoiceCount { get; set; }

    public long SizeInBytes { get; set; }

    public string FileName { get; set; } = default!;

    public string StoragePath { get; set; } = default!;

    public DateTime CreatedAt { get; set; }

    public ArchiveStatus Status { get; set; }
}
```

Status:

```text
Created
Processing
Completed
Failed
```

---

# 22. Archive Worker Akışı

Temel algoritma:

```text
START
  │
  ▼
Elasticsearch'ten batch al
  │
  ▼
Batch boş mu?
  │
  ├── YES → FINISH
  │
  └── NO
       │
       ▼
    ZIP oluştur
       │
       ▼
    ZIP'i storage'a gönder
       │
       ▼
    Kafka event üret
       │
       ▼
    next batch
       │
       └───────────────┐
                       │
                       ▼
                     tekrar
```

---

# 23. Concurrency

Tek worker:

```text
Batch 1
 ↓
Batch 2
 ↓
Batch 3
```

çalışabilir.

Performans gerektiğinde:

```text
               ┌── Batch 1
Elasticsearch ├── Batch 2
               ├── Batch 3
               └── Batch 4
```

şeklinde paralel processing yapılabilir.

Örneğin:

```text
MaxConcurrency = 4
```

ile başlanabilir.

Ancak concurrency değeri Elasticsearch ve storage üzerindeki yük ölçülerek belirlenmelidir.

---

# 24. Backpressure

Elasticsearch hızlı, ZIP oluşturma yavaşsa worker sınırsız batch almamalıdır.

Örneğin:

```text
Elasticsearch
     ↓
████████████
     ↓
ZIP
     ↓
Kafka
```

Bu durumda RAM kullanımı artabilir.

.NET tarafında bounded `Channel<T>` kullanılabilir:

```text
Channel<ArchiveBatch>
Capacity = 4
```

Böylece aynı anda bellekte tutulabilecek batch sayısı sınırlandırılabilir.

---

# 25. Arşivlenecek Veri

Aşağıdaki konu kesinleştirilmelidir.

Elasticsearch document:

```json
{
  "invoiceId": "...",
  "uuid": "...",
  "sender": "...",
  "receiver": "...",
  "xml": "...",
  "createdAt": "..."
}
```

ise ZIP'e:

### Seçenek 1 — Sadece XML

```text
invoice.xml
```

### Seçenek 2 — JSON

```text
invoice.json
```

### Seçenek 3 — XML + Metadata

```text
invoice.xml
metadata.json
```

E-fatura arşivi açısından XML'in kendisinin saklanması muhtemelen ana gereksinim olacaktır; ancak proje başlangıcında kesinleştirilmelidir.

---

# 26. Archive Manifest

ZIP içerisinde `manifest.json` bulunması önerilir.

Örneğin:

```json
{
  "batchId": "20260811-000001",
  "invoiceCount": 10000,
  "createdAt": "2026-08-11T10:20:30Z",
  "firstInvoiceId": "1000001",
  "lastInvoiceId": "1010000",
  "checksum": "..."
}
```

Bu sayede archive integrity kontrol edilebilir.

---

# 27. Checksum

ZIP için SHA-256 checksum üretilebilir.

Kafka event:

```json
{
  "batchId": "20260811-000001",
  "fileName": "batch-001.zip",
  "size": 524288000,
  "sha256": "abc123..."
}
```

şeklinde olabilir.

Böylece storage'a yazılan arşivin bütünlüğü doğrulanabilir.

---

# 28. İşlem Durumu Takibi

Archive metadata/status için PostgreSQL gibi ayrı bir database kullanılabilir.

Örneğin:

```text
archive_batches
```

tablosu:

```text
id
batch_id
invoice_count
file_name
file_size
storage_path
status
retry_count
created_at
completed_at
error_message
```

Bu tablo üzerinden:

```text
Kaç batch işlendi?
Kaç batch başarısız?
Hangileri retry bekliyor?
Hangi batch S3'e yazıldı?
```

gibi sorular cevaplanabilir.

---

# 29. Kritik Konu: Elasticsearch Verisi Silinecek mi?

Arşivleme sonrasında Elasticsearch'teki kayıtların silinip silinmeyeceği kesinleştirilmelidir.

Eğer silinecekse:

```text
Elasticsearch
     ↓
ZIP
     ↓
S3 SUCCESS
     ↓
Archive verified
     ↓
Kafka SUCCESS
     ↓
ES delete
```

şeklinde ilerlenmelidir.

**S3 upload başarılı olmadan Elasticsearch'teki kayıt silinmemelidir.**

Daha güvenli bir yöntem state machine kullanmaktır:

```text
Pending
   ↓
Archiving
   ↓
Uploaded
   ↓
Verified
   ↓
Deleted
```

---

# 30. Exactly Once Problemi

Dağıtık sistemlerde:

```text
Elasticsearch
Kafka
S3
```

arasında gerçek anlamda tek transaction yapmak kolay değildir.

Örneğin:

```text
S3 upload başarılı
Kafka commit başarısız
```

olabilir.

Consumer tekrar başladığında aynı ZIP'i tekrar upload etmek isteyebilir.

Bu yüzden sistemin temel prensibi:

> At-least-once processing + idempotent operation

olmalıdır.

---

# 31. MinIO

Lokal geliştirme ortamında AWS S3 yerine MinIO kullanılabilir.

Docker Compose:

```text
docker-compose
│
├── Elasticsearch
├── Kafka
├── MinIO
├── Kafka Connect
└── Archive Worker
```

.NET tarafında S3-compatible endpoint kullanılarak:

```text
Development → MinIO
Production  → AWS S3
```

şeklinde aynı storage abstraction kullanılabilir.

---

# 32. Docker Compose

Lokal mimari:

```text
                    Docker Compose
                         │
       ┌─────────────────┼──────────────────┐
       │                 │                  │
       ▼                 ▼                  ▼
Elasticsearch          Kafka              MinIO
       │                 │                  │
       │                 │                  │
       └────────────┬────┴──────────────┐   │
                    │                   │   │
                    ▼                   │   │
             Archive Worker             │   │
                    │                   │   │
                    └───────────────────┴───┘
```

Kafka Connect ayrıca container olarak çalıştırılabilir.

---

# 33. Gözlemlenebilirlik

Production ortamında mutlaka:

```text
Logging
Metrics
Tracing
```

düşünülmelidir.

Structured logging ile:

```text
BatchId
InvoiceCount
ZipSize
ProcessingTime
KafkaOffset
S3Path
RetryCount
```

gibi alanlar loglanabilir.

Örnek:

```text
BatchId=20260811-000001
InvoiceCount=10000
ZipSize=523MB
ProcessingTime=18.4s
Status=Completed
```

---

# 34. Metrics

Prometheus/OpenTelemetry gibi sistemlerle:

```text
archive_batches_total
archive_batches_failed_total
archive_invoices_total
archive_zip_size_bytes
archive_processing_duration_seconds
kafka_publish_duration
s3_upload_duration
```

gibi metric'ler tutulabilir.

Bu sayede:

```text
100.000 invoice kaç dakikada arşivleniyor?
```

gibi performans soruları ölçülebilir.

---

# 35. Performans Testleri

Optimum batch size ve concurrency benchmark ile belirlenmelidir.

Örnek:

| Batch | Invoice | ZIP | Süre |
|---|---:|---:|---:|
| 1 | 1.000 | 50 MB | 2 sn |
| 2 | 5.000 | 250 MB | 8 sn |
| 3 | 10.000 | 500 MB | 15 sn |
| 4 | 25.000 | 1.2 GB | 40 sn |

Buradan optimum batch size belirlenebilir.

Testlerde ayrıca:

- CPU kullanımı
- RAM kullanımı
- Elasticsearch response süresi
- ZIP compression süresi
- Kafka throughput
- S3 upload throughput
- Network kullanımı

ölçülmelidir.

---

# 36. Test Senaryoları

## Başarılı Senaryo

```text
ES
 ↓
10.000 invoice
 ↓
ZIP
 ↓
Kafka
 ↓
S3
 ↓
SUCCESS
```

## Elasticsearch Bağlantısı Koparsa

```text
ES
 ↓
ERROR
 ↓
Retry
```

## ZIP Oluşturma Başarısızsa

```text
Batch
 ↓
ZIP ERROR
 ↓
Batch FAILED
```

## Kafka Down

```text
ZIP
 ↓
Kafka unavailable
 ↓
Retry
```

## S3 Down

```text
Kafka
 ↓
Consumer
 ↓
S3 ERROR
 ↓
Retry
 ↓
DLQ
```

## Aynı Batch İki Kere Gelirse

```text
Batch 123
 ↓
S3
 ↓
already exists
 ↓
idempotent success
```

---

# 37. Güvenlik

AWS tarafında:

- IAM Role
- Least privilege
- Bucket policy
- Encryption
- Access logging

kullanılmalıdır.

Worker'ın yalnızca gerekli yetkilere sahip olması gerekir.

Örneğin sadece:

```text
s3:PutObject
```

gerekiyorsa `s3:*` verilmemelidir.

E-fatura verileri hassas olduğundan S3 Server-Side Encryption gibi mekanizmalar kullanılmalıdır.

---

# 38. S3 Lifecycle Policy

Arşivlerin uzun süre tutulması durumunda lifecycle policy kullanılabilir.

Örneğin:

```text
0-90 gün
→ S3 Standard

90-365 gün
→ S3 Standard-IA

365+ gün
→ Glacier
```

Bu sayede uzun vadeli storage maliyeti azaltılabilir.

---

# 39. Önerilen MVP

İlk versiyonda sistem gereksiz şekilde karmaşıklaştırılmamalıdır.

Önerilen MVP:

```text
.NET 8 Worker
       ↓
Elasticsearch
       ↓
Batch 5.000 / 10.000
       ↓
Streaming ZIP
       ↓
Kafka
       ↓
Kafka Consumer
       ↓
MinIO
```

Sonrasında:

```text
Retry
DLQ
Metrics
Checksum
Manifest
S3
Kafka Connect
Concurrency
Lifecycle
```

eklenebilir.

---

# 40. Production Mimarisi

Production için önerilen yapı:

```text
                         ┌───────────────────┐
                         │   Elasticsearch   │
                         └─────────┬─────────┘
                                   │
                              PIT + Search
                                   │
                                   ▼
                         ┌───────────────────┐
                         │ Archive Worker    │
                         │      .NET 8       │
                         └─────────┬─────────┘
                                   │
                            Batch Processor
                                   │
                                   ▼
                         ┌───────────────────┐
                         │ Streaming ZIP     │
                         │ + Manifest        │
                         │ + SHA256          │
                         └─────────┬─────────┘
                                   │
                                   ▼
                              AWS S3
                                   │
                                   │
                         Archive Created Event
                                   │
                                   ▼
                         ┌───────────────────┐
                         │      Kafka        │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │ Storage Consumer   │
                         │ / Kafka Connect    │
                         └─────────┬─────────┘
                                   │
                                   ▼
                         ┌───────────────────┐
                         │ Archive Metadata   │
                         │ / Status Tracking  │
                         └───────────────────┘
```

Özellikle büyük binary ZIP dosyalarının Kafka'dan geçirilmesi yerine S3/MinIO'nun binary storage olarak kullanılması ve Kafka'nın event/metadata taşıması daha ölçeklenebilir bir yaklaşımdır.

---

# 41. Projeye Başlamadan Önce Netleştirilmesi Gerekenler

## Veri

- Elasticsearch'teki hangi index'ler kullanılacak?
- Fatura document yapısı nedir?
- ZIP'in içine XML mi JSON mı konacak?
- Kaç milyon kayıt var?
- Ortalama invoice boyutu nedir?

## Batch

- Batch kaç invoice olacak?
- Maksimum ZIP boyutu olacak mı?
- Tarihe göre mi batch yapılacak?
- Tenant/company bazlı mı batch yapılacak?

## Kafka

- ZIP binary olarak mı Kafka'ya gidecek?
- Yoksa Kafka sadece event mi taşıyacak?
- Kaç partition olacak?
- Retention ne kadar?
- DLQ olacak mı?

## Storage

- Production AWS S3 mü?
- Local MinIO mu?
- Bucket yapısı nasıl olacak?
- S3 lifecycle olacak mı?
- Encryption kullanılacak mı?

## Archive

- Manifest olacak mı?
- Checksum olacak mı?
- Dosya naming standardı ne?
- Aynı batch tekrar gelirse ne olacak?

## Elasticsearch

- Arşivlenen kayıtlar silinecek mi?
- Silinecekse ne zaman?
- Silme işlemi nasıl doğrulanacak?

---

# 42. Sonuç ve Teknik Öneri

Bu proje sadece:

> Elasticsearch'ten veriyi al → ZIP yap → Kafka'ya gönder → S3'e at

şeklinde değerlendirilmemelidir.

Asıl problem, yüksek hacimli verinin güvenli, tekrar çalıştırılabilir, memory-efficient ve ölçeklenebilir şekilde arşivlenmesidir.

Önerilen temel prensipler:

1. Elasticsearch'ten pagination/streaming ile oku.
2. Verileri kontrollü batch'le.
3. ZIP'i streaming olarak oluştur.
4. ZIP boyutunu sınırla.
5. Her batch'e benzersiz `BatchId` ver.
6. Idempotency uygula.
7. Retry + DLQ kullan.
8. Büyük binary'leri mümkünse Kafka'dan geçirme.
9. Kafka'yı event orchestration için kullan.
10. S3/MinIO'yu gerçek binary storage olarak kullan.
11. Manifest + checksum ekle.
12. Archive metadata/status takibi yap.
13. Metrics ve structured logging ekle.
14. Önce MinIO + Docker Compose ile lokal benchmark yap.
15. Batch size ve concurrency değerlerini benchmark sonucuna göre belirle.

## En Kritik Mimari Karar

Gereksinimde "Kafka üzerinden AWS S3'e aktarım" denmesi, ZIP dosyasının mutlaka Kafka mesajının kendisi olması gerektiği anlamına gelmez.

İki yaklaşım bulunmaktadır:

### Yaklaşım 1

```text
Elasticsearch
    ↓
ZIP
    ↓
Kafka
    ↓
Kafka Connect
    ↓
S3
```

### Yaklaşım 2 — Önerilen

```text
Elasticsearch
    ↓
.NET Worker
    ↓
ZIP
    ↓
S3 / MinIO
    ↓
Kafka Event
    ↓
Consumer / Kafka Connect
    ↓
Metadata / Status
```

Büyük dosyalar söz konusu olduğunda **2. yaklaşımın** daha ölçeklenebilir ve operasyonel açıdan daha güvenli olması beklenir.

Bu karar, proje başlamadan önce netleştirilmesi gereken en önemli mimari konudur.
