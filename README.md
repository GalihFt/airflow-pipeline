# Catatan Belajar Apache Airflow — Daily Sales Pipeline

Repository ini adalah catatan praktik saya belajar membuat data pipeline dengan Apache Airflow. Saya mulai dari task sederhana, mengolah data transaksi menjadi laporan penjualan harian, mencoba implementasi dengan pandas, lalu menyimpan hasilnya ke SQLite.

Proyek ini berangkat dari workshop **Your First Data Pipeline with Apache Airflow** dari **Insight Receh**, menggunakan [Airflow Receh Toolkit](https://github.com/caesarmario/airflow-receh-toolkit) sebagai starter repository. Materi dan setup awal berasal dari workshop tersebut; repository ini saya gunakan untuk mencatat hasil praktik dan pengembangan selama belajar.

## Gambaran pipeline

Data transaksi dibaca dari `data/orders.csv`. Hanya transaksi dengan status `paid` yang dihitung. Transaksi dikelompokkan berdasarkan tanggal untuk menghasilkan jumlah pesanan dan total pendapatan harian.

```text
data/orders.csv
    |
    v
daily_sales_pipeline ATAU daily_sales_pipeline_pd
    extract -> transform -> validate -> publish
    |
    v
output/daily_sales.csv
    |
    v
load_daily_sales_to_sqlite
    read_report -> load_database -> check_database
    |
    v
output/sales.db (tabel daily_sales)
```

Kedua DAG pengolahan penjualan adalah alternatif implementasi dan menulis ke file output yang sama. Saya menjalankan salah satunya, menunggu hingga berhasil, lalu menjalankan DAG SQLite. Saat ini belum ada dependensi otomatis antar-DAG.

## Isi repository

```text
.
├── dags/
│   ├── my_first_pipeline.py
│   ├── daily_sales_pipeline.py
│   ├── daily_sales_pipeline_pd.py
│   └── load_daily_sales_to_sqlite.py
├── data/
│   └── orders.csv
├── output/
│   ├── daily_sales.csv   # Hasil DAG pengolahan penjualan
│   └── sales.db          # Database hasil DAG SQLite
├── docker-compose.yml
└── README.md
```

File laporan dan database di folder `output/` dibuat atau diperbarui saat pipeline dijalankan.

## DAG yang saya buat

### 1. `my_first_pipeline`

Latihan awal untuk memahami struktur DAG dan task menggunakan decorator `@dag` dan `@task` dari `airflow.sdk`. DAG ini memiliki satu task, `say_hello`, yang mencetak `Hello Bro` ke log.

### 2. `daily_sales_pipeline`

Versi pengolahan data menggunakan modul bawaan Python, yaitu `csv`.

| Task | Proses |
| --- | --- |
| `extract` | Membaca transaksi dari `data/orders.csv` menjadi list dictionary. |
| `transform` | Memilih transaksi `paid`, mengubah `amount` menjadi integer, lalu menghitung jumlah pesanan dan pendapatan per tanggal. |
| `validate` | Memastikan hasil tidak kosong, jumlah pesanan lebih dari nol, dan pendapatan tidak negatif. |
| `publish` | Menulis laporan ke `output/daily_sales.csv`. |

### 3. `daily_sales_pipeline_pd`

Versi alternatif dengan pandas, menggunakan urutan task yang sama. Saya menggunakan DataFrame untuk membaca CSV, menyaring transaksi, dan melakukan agregasi dengan `groupby` serta `agg`.

Data dikembalikan antar-task sebagai list dictionary melalui `to_dict("records")`. Versi ini membantu saya membandingkan pengolahan data dengan loop Python dan operasi DataFrame.

### 4. `load_daily_sales_to_sqlite`

DAG lanjutan untuk memasukkan laporan CSV ke database SQLite.

| Task | Proses |
| --- | --- |
| `read_report` | Membaca `output/daily_sales.csv` dan memastikan file tersedia serta memiliki data. |
| `load_database` | Membuat tabel `daily_sales` jika belum ada, menghapus isi laporan sebelumnya, lalu memasukkan seluruh baris laporan terbaru. |
| `check_database` | Membandingkan jumlah baris di database dengan jumlah baris yang dimuat dan menampilkan isinya di log. |

Kolom tabelnya adalah `date` (TEXT, primary key), `total_orders` (INTEGER), dan `revenue` (INTEGER). Proses load mengganti seluruh isi tabel, sehingga menjalankan laporan yang sama berulang kali tidak menambah baris duplikat. Database ini menyimpan laporan terbaru, bukan riwayat setiap DAG run.

## Data input dan hasil

Contoh data di `data/orders.csv`:

```csv
order_id,order_date,amount,status
O001,2026-11-01,100000,paid
O002,2026-11-01,50000,paid
O003,2026-11-01,25000,pending
O004,2026-11-02,75000,paid
O005,2026-11-02,30000,cancelled
```

Hasil agregasi di `output/daily_sales.csv`:

```csv
date,total_orders,revenue
2026-11-01,2,150000
2026-11-02,1,75000
```

Transaksi `pending` dan `cancelled` tidak masuk perhitungan. Setelah DAG SQLite berhasil, hasil yang sama dapat dibaca dari tabel `daily_sales`:

```sql
SELECT date, total_orders, revenue
FROM daily_sales
ORDER BY date;
```

Query ini bisa dijalankan menggunakan aplikasi pembaca SQLite dengan membuka `output/sales.db`.

## Cara menjalankan di lokal

Saya menggunakan Docker Desktop dan Docker Compose v2. Python dan Airflow berjalan di container, sehingga tidak perlu dipasang langsung di laptop. Image yang digunakan dalam `docker-compose.yml` adalah `apache/airflow:3.3.2` dengan perintah `standalone`.

1. Pastikan Docker Desktop sudah berjalan, lalu buka terminal di folder repository.
2. Jalankan Airflow:

   ```bash
   docker compose up -d
   ```

3. Buka [Airflow UI](http://localhost:18088). Setup lokal ini mengaktifkan akses admin tanpa login.
4. Untuk latihan awal, trigger `my_first_pipeline` dan periksa log task `say_hello`.
5. Trigger salah satu DAG: `daily_sales_pipeline` atau `daily_sales_pipeline_pd`.
6. Tunggu semua task berhasil, lalu periksa `output/daily_sales.csv`.
7. Trigger `load_daily_sales_to_sqlite`, lalu periksa log `check_database` dan file `output/sales.db`.

Semua DAG menggunakan `schedule=None` dan `catchup=False`, sehingga dijalankan manual melalui UI tanpa mengejar jadwal historis. Istilah *daily sales* merujuk pada agregasi per tanggal transaksi; saat ini pipeline belum dijadwalkan berjalan setiap hari. Setiap run membaca seluruh file input.

Untuk melihat log container ketika UI belum siap:

```bash
docker compose logs -f airflow
```

Tekan `Ctrl+C` untuk berhenti mengikuti log. Untuk menghentikan environment:

```bash
docker compose down
```

Metadata Airflow tersimpan di named volume `airflow_home`, sedangkan data input dan output terhubung ke folder lokal. Perintah `docker compose down -v` juga menghapus volume metadata Airflow, tetapi tidak menghapus file di folder lokal `output/`.

## Catatan troubleshooting

| Kendala | Hal yang saya periksa |
| --- | --- |
| Airflow UI belum terbuka | Pastikan Docker berjalan, tunggu inisialisasi, lalu cek log container. |
| Port `18088` sudah digunakan | Periksa aplikasi atau container lain yang memakai port tersebut. |
| DAG tidak muncul | Periksa file di `dags/` dan import error di Airflow. Untuk versi pandas, pastikan modul `pandas` tersedia di environment container. |
| Nilai `amount` tidak bisa dikonversi | Periksa transaksi `paid` di CSV; misalnya nilai `invalid` akan membuat task `transform` gagal. |
| Validasi gagal | Periksa apakah ada transaksi `paid`, jumlah pesanan valid, dan total pendapatan per tanggal tidak negatif. |
| Laporan tidak ditemukan saat load SQLite | Jalankan salah satu DAG pengolahan penjualan hingga berhasil terlebih dahulu. |
| File output ada tetapi run gagal | File dapat berasal dari run sebelumnya; periksa status run dan log terbaru. |
| Permission denied pada output di Linux | Atur `AIRFLOW_UID` di `.env` sesuai UID pengguna, lalu jalankan ulang Compose. |

Contoh pengaturan UID untuk Linux:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d
```

## Hal yang saya pelajari

- Menyusun urutan task dan dependensi data dengan Airflow TaskFlow API.
- Memisahkan proses extract, transform, validate, dan publish agar kegagalan mudah ditelusuri lewat log.
- Mengolah data yang sama menggunakan modul `csv` dan pandas.
- Memuat laporan ke SQLite menggunakan query berparameter dan transaksi database.
- Memeriksa hasil pipeline melalui file CSV, isi database, dan status task di Airflow.

Setup ini ditujukan untuk latihan lokal. Autentikasi disederhanakan dan port UI diikat ke `127.0.0.1`; konfigurasi ini bukan untuk penggunaan production atau akses publik.
