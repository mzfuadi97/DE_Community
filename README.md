# Data Pipeline Project

## Struktur Repository

```
project_root/
├── src/
│   ├── extractors/
│   ├── transformers/
│   ├── loaders/
│   └── utils/
├── tests/
├── config/
├── docs/
├── requirements.txt
├── main.py
└── README.md
```

## Production-Ready ETL System: Advanced Features

Project ini dirancang agar mudah dikembangkan menjadi ETL system siap produksi dengan fitur-fitur berikut:

### 1. Configuration Management
- **YAML/JSON config files:**
  - Semua konfigurasi pipeline (data sources, transformasi, tujuan load, dsb) didefinisikan di `config/config.yaml`.
- **Environment variable support:**
  - Dapat dikembangkan dengan membaca variabel environment untuk parameter sensitif (misal: kredensial, bucket S3, dsb).
- **Config validation:**
  - Validasi konfigurasi dapat ditambahkan di `config/config.py` untuk memastikan semua field wajib sudah terisi dan format benar.
- **Multiple environment setup (dev/staging/prod):**
  - Struktur config mendukung banyak environment, misal dengan file `config_dev.yaml`, `config_prod.yaml`, atau field `env` di YAML.

### 2. Data Pipeline Architecture
- **Modular design dengan Extract/Transform/Load classes:**
  - Kode pipeline dipisah ke dalam modul: `src/extractors/`, `src/transformers/`, `src/loaders/`.
- **Plugin architecture untuk different data sources:**
  - Mudah menambah extractor baru untuk berbagai sumber data (misal: database, API, file) dengan menambah modul di `src/extractors/`.
- **Retry mechanisms untuk failed operations:**
  - Dapat diimplementasikan di modul `src/utils/retry.py` untuk membungkus operasi rawan gagal (misal: upload S3, baca file).
- **Parallel processing capabilities:**
  - Pipeline dapat dikembangkan untuk memproses data secara paralel (misal: dengan multiprocessing/threading di Python) pada tahap extract, transform, atau load.

### 3. Monitoring & Alerting
- **Pipeline performance metrics:**
  - Modul `src/utils/monitoring.py` dapat digunakan untuk mencatat waktu eksekusi, throughput, dsb.
- **Data quality monitoring:**
  - Dapat menambah validasi data (misal: cek missing value, tipe data) di tahap transformasi.
- **Error alerting system:**
  - Modul `src/utils/alerting.py` dapat dikembangkan untuk mengirim notifikasi (email, Slack, dsb) jika terjadi error.
- **Execution time tracking:**
  - Pipeline dapat mencatat waktu mulai/selesai setiap tahap dan menyimpan log-nya.

---

## Persyaratan

- Python 3.8 atau lebih baru
- pip (Python package manager)
- Akses ke AWS S3 (jika ingin upload ke S3)
- File data: `user_activities.jsonl` dan `api_logs.jsonl` di root project

## Setup Environment

1. **Buat Virtual Environment (Opsional tapi Disarankan)**
   ```bash
   python -m venv venv
   ```

2. **Aktifkan Virtual Environment**
   - **Windows:**
     ```bash
     venv\Scripts\activate
     ```
   - **Mac/Linux:**
     ```bash
     source venv/bin/activate
     ```

3. **Install Dependencies**
   - Pastikan ada file `requirements.txt` (lihat contoh di bawah).
   - Install dengan:
     ```bash
     pip install -r requirements.txt
     ```

   **Contoh isi `requirements.txt`:**
   ```
   pandas
   boto3
   ```

4. **Konfigurasi AWS (Jika ingin upload ke S3)**
   - Jalankan:
     ```bash
     aws configure
     ```
   - Masukkan AWS Access Key, Secret Key, region, dan output format sesuai kebutuhan.

5. **Cek/Masukkan File Data**
   - Pastikan file `user_activities.jsonl` dan `api_logs.jsonl` ada di root project.

## Menjalankan Pipeline

Jalankan perintah berikut di terminal:
```bash
python main.py
```

## Output

- File hasil join dan agregasi akan muncul di root project, misal:
  - `output_data.json`
  - `action_counts.json`
  - `page_visit_counts.json`
  - `device_counts.json`
  - `avg_time_diff_per_user.json`
  - `status_code_counts.json`
  - `avg_response_time_per_endpoint.json`
  - `request_counts_per_user.json`
- Semua file output juga otomatis diupload ke S3 (jika konfigurasi benar).

## Testing

- Tempatkan script/unit test di folder `tests/`.
- Jalankan dengan pytest/unittest sesuai kebutuhan.

---

**Catatan:**  
- Jika ada perubahan struktur folder, pastikan semua import di file Python sudah disesuaikan.
- Untuk pengembangan lebih lanjut, tambahkan dependensi ke `requirements.txt` sesuai kebutuhan. 

__pycache__/
*.py[cod]
*$py.class
venv/
belajarde/
.vscode/
.DS_Store
.ipynb_checkpoints
*.log
*.jsonl
*.json
output_data.json