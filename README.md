Bu metin, geliştireceğiniz **Asenkron Akademik Veri Madenciliği Projesi** için baştan sona tüm mimariyi, kullanılacak modern araçları ve takip edilecek adımları içeren resmi kılavuzunuzdur (Kutup Yıldızı). Projenin her aşamasında bu dokümana geri dönerek rotanızı kontrol edebilirsiniz.

---

# Proje Kutup Yıldızı: Asenkron Akademik Veri Madenciliği Aracı

## 1. Mimari Tasarım ve Teknoloji Yığını (Tech Stack)

Projenin temel felsefesi: **Maksimum performans, minimal bağımlılık, asenkron iş akışı ve veri güvenilirliğidir.**

* **Proje ve Paket Yönetimi:** `uv` (Rust ile yazılmış, ultra hızlı yeni nesil paket yöneticisi).
* **Kod Kalitesi ve Format:** `ruff` (Rust ile yazılmış, linter ve formatter standardı).
* **Ağ ve Bağlantı (HTTP):** `httpx` (Asenkron HTTP istekleri için).
* **HTML Parçalama (Parsing):** `selectolax` (C tabanlı Modest motoru kullanan, en hızlı HTML ayrıştırıcı).
* **Akademik Veri Bağlantıları:** OpenAlex API (Doğrudan `httpx` ile asenkron sorgular).
* **PDF İşleme:** `opendataloader-pdf` (Hybrid Mode: Basit sayfalar için hızlı Java motoru, karmaşık tablolar/OCR için yerel `docling` sunucusu).
* **Veri Manipülasyonu:** `polars` (Rust tabanlı, bellek dostu ve yüksek performanslı veri çerçevesi).
* **Veritabanı:** `sqlite3` (Yerel, hafif, ilişkisel ve kurşun geçirmez depolama).
* **Takip ve Günlükleme:** `loguru` (Modern loglama) + `tqdm` (Hafif ilerleme çubuğu).

---

## 2. Proje Dosya Yapısı (Folder Structure)

Modern Python standartlarına ve `uv` yapısına uygun, temiz ve modüler dosya düzeni:

```text
dergipark_miner/
├── .venv/                      # uv tarafından yönetilen sanal ortam
├── data/
│   ├── raw_pdfs/               # İndirilen ham PDF makaleleri
│   ├── parsed_jsons/           # PDF'lerden çıkarılan yapılandırılmış metinler
│   └── Sobiad_Mining.sqlite    # SQLite yerel veritabanı dosyası
├── src/
│   ├── __init__.py
│   ├── crawler.py              # httpx ve selectolax ile sayfa/metadata kazıma
│   ├── pdf_processor.py        # opendataloader-pdf ve docling entegrasyonu
│   ├── storage.py              # polars ve sqlite3 veritabanı işlemleri
│   ├── utils.py                # Ortak yardımcı fonksiyonlar (Telegram bot vb.)
│   └── main.py                 # Ana orkestrasyon ve döngü yönetimi (tqdm + loguru)
├── tests/                      # Birim testleri
│   └── __init__.py
├── checkpoint.json             # Kod kesintiye uğrarsa kalınan yeri tutan dosya
├── pyproject.toml              # Proje bağımlılıkları, uv ve ruff ayarları
└── README.md                   # Proje genel açıklaması ve çalıştırma kılavuzu

```

---

## 3. Adım Adım Geliştirme Yol Haritası

### Faz 1: Proje Ortamı ve Standartların Kurulması

1. **uv ile Projeyi Başlatın:** Proje klasöründe `uv init` komutunu çalıştırarak modern Python ortamını oluşturun.
2. **Bağımlılıkları Ekleyin:** `uv add httpx selectolax polars loguru tqdm` komutları ile ana kütüphaneleri yükleyin. `pip install -U "opendataloader-pdf[hybrid]"` ile PDF motorunu kurun.
3. **Geliştirici Araçlarını Yapılandırın:** `uv add --dev ruff` ile kod denetleyicisini ekleyin. `pyproject.toml` içerisine ruff kurallarını (line-length, select vb.) ekleyerek her kod yazımından önce `ruff check` ve `ruff format` komutlarıyla kodu temizleyin.

### Faz 2: Veri Toplama ve Kazıma Motoru (src/crawler.py)

1. **OAI-PMH Entegrasyonu:** DergiPark'ın toplu metadata sağlayan OAI-PMH XML servislerine `httpx` ile asenkron istekler atın.
2. **HTML Parse Fonksiyonları:** XML'de eksik kalan spesifik alanlar (anahtar kelimeler, DOI vb.) için makale detay sayfalarını `httpx` ile asenkron indirin ve `selectolax` CSS seçicileri ile ışık hızında ayıklayın.
3. **OpenAlex API Bağlantısı:** DOI numarası alınan makalelerin uluslararası atıf ağlarını ve yazar eşleştirmelerini doğrulamak için `httpx` üzerinden doğrudan OpenAlex API'sine asenkron sorgular gönderin.

### Faz 3: Hibrit PDF İşleme Hattı (src/pdf_processor.py)

1. **PDF İndirme Sistemi:** `crawler.py` tarafından bulunan PDF linklerini `httpx` akış (stream) modunda `data/raw_pdfs/` klasörüne asenkron olarak indirin.
2. **Docling Sunucusunu Başlatma:** Arka planda `opendataloader-pdf-hybrid --port 5002 --force-ocr --ocr-lang "tr,en"` komutu ile yerel AI tabanlı docling sunucusunu ayağa kaldırın.
3. **Parsing İşlemi:** İndirilen PDF'leri `opendataloader_pdf.convert(..., hybrid="docling-fast")` fonksiyonuna besleyin. Basit sayfaların Java ile hızlıca, tablolu sayfaların ise Docling ile yüksek doğrulukla JSON'a dönüştürülmesini sağlayın.

### Faz 4: Veri Temizleme, Modelleme ve Depolama (src/storage.py)

1. **Polars ile Normalizasyon:** Kazınan metadataları ve PDF'lerden çıkan JSON çıktılarını `polars.DataFrame` yapısına alın. Eksik verileri temizleyin, yazar isimlerini standardize edin ve tarih formatlarını düzenleyin.
2. **SQLite Şema Tasarımı:** Veritabanında `journals` (dergiler), `articles` (makaleler), `authors` (yazarlar) ve `citations` (atıflar) tablolarını (Primary/Foreign Key ilişkileriyle) oluşturun.
3. **Veritabanına Yazma:** Polars ile temizlenen verileri `sqlite3` veritabanına bulk (toplu) veya sıralı `INSERT` sorgularıyla kalıcı olarak yazın.

### Faz 5: Orkestrasyon, Loglama ve Kesinti Yönetimi (src/main.py)

1. **Ana Döngü:** Tüm bileşenleri `main.py` içinde birleştirin. Binlerce makalelik listeyi `tqdm` ilerleme çubuğu içine alarak terminalde anlık hız ve kalan süre takibi yapın.
2. **Hata Yakalama (Robustness):** Her asenkron isteği ve PDF işleme adımını `try-except` blokları içine alın. İnternet kopması veya hatalı veri durumunda kodun çökmesini engelleyin; başarısız işlemleri `loguru` ile `data/error.log` dosyasına kaydedin.
3. **Checkpoint (Kaldığı Yerden Devam):** Her 50-100 başarılı kayıtta bir, en son işlenen makale ID'sini `checkpoint.json` dosyasına yazın. Sistem kapansa bile yeniden başlatıldığında bu dosyayı okuyarak kaldığı yerden devam etmesini sağlayın.

---

## 4. Final Hedefi ve Başarı Kriterleri

Bu projenin başarıyla tamamlandığının göstergeleri şunlardır:

1. **Hız:** Asenkron mimari sayesinde geleneksel Selenium/Requests tabanlı sistemlere kıyasla en az 10 kat daha hızlı veri toplayabilmesi.
2. **Doğruluk:** `opendataloader-pdf` hibrit modu sayesinde makale içindeki tabloların yapısı bozulmadan veritabanına aktarılabilmesi.
3. **Dayanıklılık:** Günlerce çalışsa dahi bellek sızıntısı (memory leak) yapmaması, internet kesintilerinde kendi kendine yeniden deneme (`retry`) mekanizmasını çalıştırabilmesi ve çöktüğünde veri kaybetmeden kaldığı yerden devam edebilmesi.
4. **Temiz Kod Düzeni:** Projenin her aşamasında `ruff` standartlarına uyulmuş, modüler ve kurumsal kalitede bir repo teslim edilmesi.
