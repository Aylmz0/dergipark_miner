# dergipark-miner

Asenkron akademik veri madenciliği aracı. DergiPark'taki tüm dergileri (`~2,574`) keşfeder, OAI-PMH ile makaleleri hasat eder, PDF'leri indirir, HTML meta etiketlerinden zengin metadata çıkarır ve tek bir JSONL dosyasına dışa aktarır. Checkpoint tabanlı resume, her aşamada veri koruması sağlar.

## Teknoloji Yığını

- **Python 3.14** + `uv` paket yöneticisi
- `httpx` — asenkron HTTP (OAI-PMH + PDF + HTML scraping)
- `selectolax` — C tabanlı HTML/XML parsing
- `loguru` — yapılandırılmış loglama
- `sqlite3` + WAL mode — yerel depolama
- `ruff` — lint ve format
- `pytest` — testler

## Proje Yapısı

```
dergipark_miner/
├── main.py                    # CLI entry point + orkestrasyon
├── src/
│   ├── crawler.py             # OAI-PMH harvest, PDF download, HTML scraping
│   ├── exporter.py            # JSONL dışa aktarma (single JOIN + GROUP_CONCAT)
│   ├── storage.py             # SQLite şema, insert/upsert, indeksler
│   └── utils.py               # retry, AdaptiveDelay, checkpoint I/O
├── tests/                     # 59 birim testi
├── data/
│   ├── Sobiad_Mining.sqlite   # SQLite veritabanı
│   ├── raw_pdfs/              # İndirilen PDF dosyaları
│   ├── exports/               # Dışa aktarılan JSONL
│   └── pdf_errors.json        # Kalıcı başarısız PDF'ler
└── checkpoint.json            # Pipeline durumu (resume)
```

## Kullanım

```bash
# Tam pipeline (tüm dergiler)
.venv/bin/python main.py --limit 0

# İlk 6 dergiyi işle
.venv/bin/python main.py --limit 6

# Durdur (Ctrl+C) — mevcut journal tamamlanır, checkpoint kaydedilir
# Yeniden çalıştır — kaldığı yerden devam eder
```

## Pipeline Akışı

1. **Dergi Keşfi** — DergiPark'tan tüm dergileri paginasyon ile çeker (~2,574 dergi)
2. **OAI-PMH Hasat** — Her dergi için makale metadata'sını XML sayfalar halinde çeker (streaming)
3. **Parse + Insert** — XML'den `title`, `doi`, `authors`, `abstract`, `keywords`, `language` çıkarır, SQLite'a yazar
4. **PDF İndirme** — Makale sayfasından `citation_pdf_url` meta etiketini çeker, PDF'i indirir
5. **HTML Metadata** — Aynı sayfadan `journal_title`, `issn`, `publisher`, `volume`, `issue` vs. çıkarır, `is_oa = 1` atar
6. **JSONL Export** — Tüm makaleleri tek dosyaya aktarır (22 alan, profesyonel sıralama)

## Direnç Mekanizmaları

| Özellik | Detay |
|---------|-------|
| **Checkpoint Resume** | `checkpoint.json` her journal sonunda kaydedilir. Crash sonrası kaldığı yerden devam |
| **OAI-PMH Token Recovery** | `badResumptionToken` hatası = journal'ı baştan dene (max 3 kez) |
| **Journal Audit** | Her journal sonrası DB vs. expected karşılaştırma, eksik makaleler loglanır |
| **Journal Retry** | Audit başarısız journal'ları aynı çalıştırmada max 3 round yeniden dener |
| **PDF Adaptive Delay** | 429/5xx durumunda otomatik backoff (2s → 60s), başarıda decay |
| **PDF Retry Chain** | Anlık retry (3 deneme) → 5 retry round'u (30/45/60/75/90s bekleme) |
| **Content Hash Dedup** | DOI + OAI ID yoksa: `MD5(title|journal_id|date)` ile duplike engelleme |
| **Graceful Shutdown** | SIGINT/SIGTERM = mevcut journal tamamlanır, checkpoint kaydedilir |
| **Transient Error Handling** | DNS hataları, 429, 500-503 = transient, `retry_count` artmaz |

## Veritabanı Şeması

**journals:** `id`, `short_name` (UNIQUE), `name`, `article_count`

**articles (26 sütun):** `id`, `journal_id` (FK), `title`, `doi`, `publication_date`, `oai_identifier`, `abstract`, `keywords`, `language`, `journal_title`, `issn`, `publisher`, `volume`, `issue`, `firstpage`, `lastpage`, `pages`, `pdf_url`, `orcid`, `article_type`, `references`, `metadata`, `is_oa`, `enriched_at`, `content_hash`, `created_at`

**authors:** `id`, `article_id` (FK), `name`

## JSONL Çıktı Formatı (22 Alan)

```json
{
  "slug": "dergipark-org-tr-journal-name",
  "article_id": "dergipark.org.tr:article/12345",
  "article_url": "https://dergipark.org.tr/...",
  "title": "Makale Başlığı",
  "authors": ["Yazar 1", "Yazar 2"],
  "journal_title": "Dergi Adı",
  "issn": "1234-5678",
  "publisher": "Yayınevi",
  "volume": "10",
  "issue": "2",
  "firstpage": "1",
  "lastpage": "20",
  "pages": "1-20",
  "date": "2024",
  "language": "tur",
  "doi": "10.xxxx/xxxxx",
  "abstract": "Makale özeti...",
  "keywords": ["anahtar", "kelime"],
  "is_oa": 1,
  "pdf_url": "https://dergipark.org.tr/.../article.pdf",
  "orcid": "0000-0000-0000-0000",
  "article_type": "research-article",
  "references": "...>"
}
```

## Testler

```bash
.venv/bin/python -m pytest tests/ -v
```

59 test: crawler (metadata parsing, PDF error handling, HTML scraping), exporter (JSONL structure, sorting, idempotent), storage (upsert, content hash dedup, author write), utils (retry, checkpoint, AdaptiveDelay).

## Web Paneli (İzleme Arayüzü)

Miner'ın SQLite veritabanını okuyan salt-okunur FastAPI paneli: dashboard, dergi/makale listeleri, arama (Türkçe karakter duyarsız), hatalar ve JSONL export indirme. Miner çalışırken WAL sayesinde sorunsuz okur.

```bash
# Geliştirme
uv run python -m src.web.app --port 8000
# veya
uv run uvicorn src.web.app:app --host 127.0.0.1 --port 8000
```

- Teknoloji: FastAPI + Jinja2 + HTMX + Alpine.js + Pico.css (vendor `static/vendor/` içinde, CDN yok)
- Sayfalar: `/` dashboard · `/journals` dergiler · `/journals/{short_name}` dergi detayı · `/articles` makaleler (arama+filtre) · `/articles/{id}` makale detayı · `/failures` hatalar · `/exports` + `/api/export.jsonl` indirme · `/search` birleşik arama
- Panel salt-okunurdur: DB'ye asla yazmaz (`mode=ro` URI), miner'a dokunmaz

### Sunucuya Yükleme (Caddy + tmux)

```bash
# 1) Kod + veri (PDF'ler hariç) sunucuya rsync ile it
rsync -avz --progress \
  --exclude '.venv/' --exclude '.git/' --exclude '__pycache__/' \
  --exclude 'graphify-out/' --exclude '.pytest_cache/' --exclude '.ruff_cache/' \
  --exclude 'data/raw_pdfs/' --exclude 'data/parsed_jsons/' \
  --exclude 'data/Sobiad_Mining.sqlite-wal' --exclude 'data/Sobiad_Mining.sqlite-shm' \
  ./ yilmaz@SUNUCU:/home/yilmaz/dergipark-miner/

# 2) Sunucuda
cd /home/yilmaz/dergipark-miner
uv sync                               # bağımlılıkları kurar (Python 3.14)
tmux new -s miner-web
uv run python -m src.web.app --port 8082   # 127.0.0.1'e bağlanır

# 3) Caddyfile
# dergipark.yilmazcs.xyz {
#     reverse_proxy localhost:8082
#     encode zstd gzip
# }
# caddy reload
```

Port 8082 örnek; Caddy yapılandırmanızda boş herhangi bir port olur. Doğrulama: `curl localhost:8082/healthz` → `{"ok": true, "db_exists": true}`.
