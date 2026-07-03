import os
import re
import sys
import redis
import time
import requests

def show_tutorial():
    print("Pilih salah satu dari pilihan berikut:")
    print("  python consumer_worker.py INA")
    print("  python consumer_worker.py SG")
    print("  python consumer_worker.py JPN")
    print()

if len(sys.argv) < 2:
    show_tutorial()
    sys.exit(1)

# ==========================
# Konfigurasi Region
# ==========================
REGION = sys.argv[1].upper()

# Alamat gateway tiap region, dipakai untuk mengirim REPLIKA lintas region.
REGIONS_CONFIG = {
    "INA": "http://localhost:5001",
    "SG":  "http://localhost:5002",
    "JPN": "http://localhost:5003"
}

if REGION not in REGIONS_CONFIG.keys():
    print(f"Region '{REGION}' tidak dikenal!")
    show_tutorial()
    sys.exit(1)

db_mapping = {
    "INA": 0,
    "SG": 1,
    "JPN": 2
}

# ==========================
# Konfigurasi Storage
# ==========================
# Path absolut berbasis lokasi script ini -> harus sama persis dengan
# BASE_DIR di server_gateway.py supaya kedua proses saling menemukan file.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# 1 folder per region -> gudang milik region ini sendiri.
STORAGE_DIR = os.path.join(BASE_DIR, f"Region{REGION}")
TEMP = os.path.join(BASE_DIR, "gateway")

os.makedirs(STORAGE_DIR, exist_ok=True)
os.makedirs(TEMP, exist_ok=True)

# ==========================
# Koneksi Redis
# ==========================
redis_client = redis.Redis(
    host="localhost",
    port=6379,
    db=db_mapping[REGION],
    decode_responses=True,
    protocol=2,
    socket_timeout=10,          # baca socket max 10 detik, jangan nunggu tanpa batas
    socket_connect_timeout=5,
    socket_keepalive=True,      # cegah koneksi idle "dibuang diam-diam" oleh OS/proxy
    retry_on_timeout=True,
    health_check_interval=30,   # ping berkala biar koneksi idle tetap dianggap hidup
)

SEP = "::"  # harus sama dengan SEP di server_gateway.py

# ==========================
# Merge Chunk
# ==========================
def chunk_sort_key(chunk_name):
    """Ambil angka index dari nama file 'chunk_<idx>.bin' dengan aman (regex),
    supaya tidak crash kalau formatnya sedikit berbeda."""
    match = re.search(r"chunk_(\d+)", chunk_name)
    return int(match.group(1)) if match else 0

def merge_chunks(region_name, upload_id, chunks, original_file):
    chunks = sorted(chunks, key=chunk_sort_key)

    output_filename = f"{region_name}_{upload_id}_{original_file}"
    output_path = os.path.join(STORAGE_DIR, output_filename)

    try:
        with open(output_path, "wb") as out:
            for c in chunks:
                chunk_file_name = f"{region_name}_{upload_id}_{c}"
                chunk_path = os.path.join(TEMP, chunk_file_name)

                if os.path.exists(chunk_path):
                    with open(chunk_path, "rb") as f:
                        out.write(f.read())
                    os.remove(chunk_path)
                else:
                    print(f"⚠️ Chunk tidak ditemukan: {chunk_path} -> merge dibatalkan untuk upload {upload_id}")
                    return False
    except OSError as e:
        print(f"❌ Gagal menulis hasil merge: {e}")
        return False

    print(f"\n[Worker-{region_name}] 🛠️ Merge sukses untuk upload {upload_id}!")
    print(f"[Worker-{region_name}] File disimpan di: {output_path}")

    register_catalog(region_name, upload_id, original_file, output_filename, is_replica=False, source_region=region_name)
    replicate_cross_region(output_path, upload_id, original_file, output_filename)
    return True

# ==========================
# File Catalog (untuk keperluan download)
# ==========================
def register_catalog(region_name, upload_id, original_file, stored_filename, is_replica, source_region):
    """Simpan metadata file secara PERSISTEN (tidak dihapus setelah merge),
    supaya gateway di region ini bisa melayani /list-files dan /download nanti."""
    try:
        redis_client.hset(f"catalog:{upload_id}", mapping={
            "upload_id": upload_id,
            "region": region_name,
            "original_filename": original_file,
            "stored_filename": stored_filename,   # nama fisik di disk
            "uploaded_at": time.time(),
            "is_replica": "1" if is_replica else "0",
            "replicated_from": source_region,
        })
        redis_client.sadd("catalog:index", upload_id)
        print(f"[Worker-{region_name}] 📚 Katalog diperbarui untuk upload {upload_id}")
    except redis.exceptions.RedisError as e:
        print(f"⚠️ Gagal menyimpan katalog (file tetap tersimpan di disk): {e}")

# ==========================
# Replikasi Lintas Region
# ==========================
def replicate_cross_region(file_path, upload_id, original_file, stored_filename):
    """Kirim salinan file ke gateway region LAIN, supaya file tetap bisa
    diakses walau region asal (region ini) mati total."""
    for target_region, target_url in REGIONS_CONFIG.items():
        if target_region == REGION:
            continue  # jangan kirim ke diri sendiri

        try:
            with open(file_path, "rb") as f:
                files = {'file': (stored_filename, f)}
                data = {
                    'upload_id': upload_id,
                    'original_filename': original_file,
                    'stored_filename': stored_filename,
                    'source_region': REGION,
                }
                res = requests.post(f"{target_url}/receive-replica", files=files, data=data, timeout=10)

            if res.status_code == 200:
                print(f"[Replication] 🚀 Berhasil replikasi ke region {target_region}")
            else:
                print(f"[Replication] ⚠️ Region {target_region} menolak replika: {res.status_code}")
        except requests.exceptions.RequestException as e:
            # Region tujuan sedang mati/tidak terjangkau -> replika ke sana
            # dilewati untuk saat ini (file tetap aman di region asal).
            print(f"[Replication] ❌ Region {target_region} tidak terjangkau, replika dilewati: {e}")

# ==========================
# Worker
# ==========================
def start_worker():
    print(f"🤖 Worker {REGION} aktif...")
    print("Menunggu task dari Redis Queue...\n")

    # Chunk dikelompokkan per upload_id, supaya beberapa upload paralel
    # yang berjalan bersamaan ke region yang sama tidak saling bercampur.
    sessions = {}  # { upload_id: [chunk_name, ...] }

    while True:
        try:
            raw_task = redis_client.lpop("task_queue")
        except redis.exceptions.RedisError as e:
            print(f"❌ Redis error, mencoba lagi dalam 2 detik: {e}")
            time.sleep(2)
            continue

        if raw_task is None:
            # Antrean kosong -> tunggu sebentar lalu poll lagi (bukan error).
            time.sleep(0.5)
            continue

        if SEP not in raw_task:
            print(f"⚠️ Task tidak dikenal formatnya, dilewati: {raw_task}")
            continue

        upload_id, chunk_name = raw_task.split(SEP, 1)
        print(f"[Worker-{REGION}] 📩 Menerima {chunk_name} (upload {upload_id})")

        sessions.setdefault(upload_id, []).append(chunk_name)

        total_expected = redis_client.get(f"meta:{upload_id}:total_chunks")
        original_file = redis_client.get(f"meta:{upload_id}:original_filename") or "output.txt"
        total_expected = int(total_expected) if total_expected else 0

        received_chunks = sessions[upload_id]

        if total_expected > 0 and len(received_chunks) == total_expected:
            print(f"\n[Worker-{REGION}] Semua chunk diterima untuk upload {upload_id} ({total_expected} buah).")
            print("[Worker] Memulai proses merge...\n")

            merge_chunks(REGION, upload_id, received_chunks, original_file)

            redis_client.delete(f"meta:{upload_id}:total_chunks")
            redis_client.delete(f"meta:{upload_id}:original_filename")
            del sessions[upload_id]

# ==========================
# Main
# ==========================
if __name__ == "__main__":
    start_time = time.time()

    try:
        start_worker()
    except KeyboardInterrupt:
        elapsed = time.time() - start_time
        print(f"\nWorker dihentikan. Runtime: {elapsed:.2f} detik")
    print()