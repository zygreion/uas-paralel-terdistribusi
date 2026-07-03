import os
import threading
import time
import uuid
import requests
import sys

# Membaca argumen nama region dan port saat script dijalankan (Contoh: python client.py INA parallel)
def show_tutorial():
    print("Cara jalankan: python client.py (INA | SG | JPN) (parallel | sequential)")
    print("Contoh: python client.py INA parallel")
    print()

if len(sys.argv) < 2:
    show_tutorial()
    sys.exit(1)

USER_REGION = sys.argv[1].upper()
PARALLEL = sys.argv[2].lower() == "parallel"

FILE = "video.txt"
CHUNK_SIZE = 10

# Daftar kluster server regional yang aktif di infrastruktur
REGIONS_CONFIG = {
    "INA": "http://localhost:5001",
    "SG":  "http://localhost:5002",
    "JPN": "http://localhost:5003"
}

if USER_REGION not in REGIONS_CONFIG.keys():
    print(f"Region '{USER_REGION}' tidak dikenal!")
    show_tutorial()
    sys.exit(1)

# Simulasi "jarak" antar region (bukan koordinat asli, cukup relatif).
# Dipakai untuk mensimulasikan delay latency jaringan, karena semua
# Region ini sebenarnya sama-sama di localhost (tidak ada beda waktu asli).
REGION_DISTANCE = {
    "INA": {"INA": 0, "SG": 1, "JPN": 3},
    "SG":  {"INA": 1, "SG": 0, "JPN": 2},
    "JPN": {"INA": 3, "SG": 2, "JPN": 0},
}
DELAY_PER_UNIT = 0.5  # detik, delay simulasi per 1 unit jarak

# ==========================
# Regional Load Balancer
# ==========================
def get_best_region_url():
    """Fungsi Cerdas Load Balancing: Memilih server dengan beban kerja paling ringan"""
    best_region_url = None
    lowest_load = float('inf')
    chosen_name = "None"

    print("\n🔍 [Load Balancer] Mengecek beban kesehatan di seluruh regional...")

    for region_name, url_base in REGIONS_CONFIG.items():
        try:
            res = requests.get(f"{url_base}/get-load", timeout=2).json()
            current_load = res["active_queue_count"]
            print(f"   -> Regional {region_name} | Sisa Antrean: {current_load} | Status: {res['status']}")

            if current_load < lowest_load:
                lowest_load     = current_load
                best_region_url = url_base
                chosen_name     = region_name
        except requests.exceptions.ConnectionError:
            print(f"   -> Regional {region_name} | ❌ OFFLINE / Mati Lampu")

    if best_region_url:
        print(f"🚀 [Decision] Mengarahkan lalu lintas data ke Region Terbaik: {chosen_name}\n")
    return best_region_url

# ==========================
# CDN Routing (home region) + Load Balancer sebagai fallback
# ==========================
def get_target_region(user_region):
    """CDN: coba arahkan ke home region user dulu (cek ALIVE saja, bukan load).
    Kalau home region mati -> baru jatuh ke Load Balancer (region lain paling ringan)."""
    if user_region in REGIONS_CONFIG:
        home_url = REGIONS_CONFIG[user_region]
        try:
            requests.get(f"{home_url}/get-load", timeout=2)
            print(f"🏠 [CDN] Home region {user_region} hidup, langsung dipakai (skip load balancing)\n")
            return home_url, user_region
        except requests.exceptions.RequestException:
            print(f"⚠️ [CDN] Home region {user_region} mati, jatuh ke Load Balancer...")

    fallback_url = get_best_region_url()
    if not fallback_url:
        return None, None

    # Cari nama region dari URL yang dipilih Load Balancer
    fallback_name = next((name for name, url in REGIONS_CONFIG.items() if url == fallback_url), None)
    return fallback_url, fallback_name

# ==========================
# Chunk Sender
# ==========================
def send_chunk_http(url_target, target_region, upload_id, chunk_name, chunk_data):
    try:
        # Simulasi latency jaringan berdasarkan "jarak" user ke region tujuan.
        # Home region (jarak 0) -> tanpa delay. Makin jauh, makin lambat.
        distance = REGION_DISTANCE.get(USER_REGION, {}).get(target_region, 2)  # default 2 kalau tidak dikenal
        delay = distance * DELAY_PER_UNIT
        time.sleep(delay)

        files = {'file': (chunk_name, chunk_data)}
        # upload_id WAJIB disertakan supaya gateway/worker tahu chunk ini milik upload yang mana
        data = {'upload_id': upload_id}
        response = requests.post(f"{url_target}/upload-chunk", files=files, data=data, timeout=10)
        if response.status_code == 200:
            print(f"[Client-Thread] ✅ Chunk Terkirim: {chunk_name} (delay simulasi: {delay:.1f}s)")
        else:
            print(f"[Client-Thread] ⚠️ Gagal ({response.status_code}) saat mengirim {chunk_name}")
    except Exception as e:
        print(f"[Client-Thread] ❌ Putus koneksi saat mengirim {chunk_name}: {e}")

# ==========================
# File Uploader
# ==========================
def start_upload(parallel=False):
    chosen_server_url, target_region = get_target_region(USER_REGION)
    if not chosen_server_url:
        print("❌ Fatal: Semua server regional mati!")
        return

    # ID unik untuk sesi upload ini, supaya tidak bentrok dengan upload lain
    # yang sedang berjalan bersamaan (paralel/distributed) di region yang sama.
    upload_id = uuid.uuid4().hex[:12]
    print(f"🆔 [Client] Upload Session ID: {upload_id}")

    threads = []
    idx = 0

    with open(FILE, "rb") as f:
        file_content = f.read()
        total_bytes = len(file_content)
        total_chunks = (len(file_content) + CHUNK_SIZE - 1) // CHUNK_SIZE

    # [METADATA] Daftarkan metadata (terikat ke upload_id) ke server regional yang terpilih
    try:
        requests.post(f"{chosen_server_url}/set-metadata", json={
            "upload_id": upload_id,
            "total_chunks": total_chunks,
            "original_filename": FILE
        }, timeout=5)
    except Exception as e:
        print(f"⚠️ Gagal mengirim metadata ke server: {e}")
        return

    start_time = time.time()

    with open(FILE, "rb") as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            chunk_name = f"chunk_{idx}.bin"

            if parallel:
                # Menggunakan Multithreading
                t = threading.Thread(target=send_chunk_http, args=(chosen_server_url, target_region, upload_id, chunk_name, data))
                t.start()
                threads.append(t)
            else:
                # Sequential
                send_chunk_http(chosen_server_url, target_region, upload_id, chunk_name, data)

            idx += 1

    if parallel:
        for t in threads:
            t.join()

    elapsed = time.time() - start_time
    distance = REGION_DISTANCE.get(USER_REGION, {}).get(target_region, "?")
    print(f"\n🏁 [Client] Selesai upload dari {USER_REGION} -> {target_region} (jarak simulasi: {distance})")
    print(f"  Tipe = {'Paralel (Multithreading)' if parallel else 'Sequential'}")
    print(f"  Execution Time = {elapsed:.2f} detik")
    print(f"  Throughput = {total_bytes} bytes / {elapsed:.2f} detik")
    print(f"             = {total_bytes / elapsed:.2f} bytes/detik")

# ==========================
# Main
# ==========================
if __name__ == "__main__":
    if not os.path.exists(FILE):
        with open(FILE, "w") as f:
            f.write("ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ")
    start_upload(parallel=PARALLEL)
    print()