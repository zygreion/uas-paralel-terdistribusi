import os
import sys
import requests

def show_tutorial():
    print("Cara jalankan: python download_client.py (INA | SG | JPN) [upload_id (opsional)]")
    print("Contoh Mode Interaktif : python download_client.py INA")
    print("Contoh Mode Direct     : python download_client.py INA bbced1cfb472")
    print()

# Membaca argumen region user (Contoh: python download_client.py INA)
if len(sys.argv) < 2:
    show_tutorial()
    sys.exit(1)

USER_REGION = sys.argv[1].upper()

# ==========================
# Konfigurasi Region
# ==========================
REGIONS_CONFIG = {
    "INA": "http://localhost:5001",
    "SG":  "http://localhost:5002",
    "JPN": "http://localhost:5003"
}

if USER_REGION not in REGIONS_CONFIG.keys():
    print(f"Region '{USER_REGION}' tidak dikenal!")
    show_tutorial()
    sys.exit(1)

# Tabel jarak yang sama seperti di client.py -> dipakai untuk memilih
# REPLIKA TERDEKAT saat 1 file punya salinan di beberapa region.
REGION_DISTANCE = {
    "INA": {"INA": 0, "SG": 1, "JPN": 3},
    "SG":  {"INA": 1, "SG": 0, "JPN": 2},
    "JPN": {"INA": 3, "SG": 2, "JPN": 0},
}

DOWNLOAD_DIR = "downloads"


def get_alive_regions():
    """Cek region mana saja yang masih hidup (skip yang mati/mati lampu)."""
    alive = {}
    print("🔍 [Client] Mengecek region yang hidup...")
    for region_name, url_base in REGIONS_CONFIG.items():
        try:
            requests.get(f"{url_base}/get-load", timeout=2).json()
            alive[region_name] = url_base
            print(f"   -> {region_name} ✅ hidup")
        except requests.exceptions.RequestException:
            print(f"   -> {region_name} ❌ mati / tidak terjangkau")
    return alive


def collect_catalog(alive_regions):
    """Kumpulkan katalog dari semua region yang hidup, lalu KELOMPOKKAN per
    upload_id -> 1 file bisa saja punya salinan (replika) di banyak region
    sekaligus karena hasil cross-region replication."""
    by_upload_id = {}
    for region_name, url_base in alive_regions.items():
        try:
            res = requests.get(f"{url_base}/list-files", timeout=5).json()
            for f in res.get("files", []):
                uid = f["upload_id"]
                f["_region_url"] = url_base
                by_upload_id.setdefault(uid, []).append(f)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Gagal ambil katalog dari {region_name}: {e}")
    return by_upload_id


def pick_nearest_copy(copies):
    """CDN: dari semua region (yang hidup) yang punya salinan file ini,
    pilih yang jaraknya paling dekat ke USER_REGION."""
    def distance_of(copy):
        return REGION_DISTANCE.get(USER_REGION, {}).get(copy["region"], 99)

    return min(copies, key=distance_of)


def show_catalog(by_upload_id):
    if not by_upload_id:
        print("📭 Tidak ada file ditemukan di region manapun yang hidup.")
        return []

    print("\n📚 Daftar file yang tersedia:")
    entries = list(by_upload_id.items())
    for i, (uid, copies) in enumerate(entries):
        original_filename = copies[0]["original_filename"]
        regions_available = ", ".join(sorted(c["region"] for c in copies))
        nearest = pick_nearest_copy(copies)
        dist = REGION_DISTANCE.get(USER_REGION, {}).get(nearest["region"], "?")
        print(f"  [{i}] {original_filename}  (upload_id: {uid})")
        print(f"       tersedia di: {regions_available}  |  🏠 terdekat dari {USER_REGION}: {nearest['region']} (jarak {dist})")
    return entries


def download(file_meta):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    url = f"{file_meta['_region_url']}/download/{file_meta['upload_id']}"
    dist = REGION_DISTANCE.get(USER_REGION, {}).get(file_meta["region"], "?")

    print(f"\n⬇️  [CDN] Mengunduh dari region terdekat: {file_meta['region']} (jarak {dist}) ...")
    try:
        with requests.get(url, stream=True, timeout=15) as r:
            if r.status_code != 200:
                print(f"❌ Gagal download: {r.status_code} - {r.text}")
                return

            out_name = file_meta["original_filename"]
            out_path = os.path.join(DOWNLOAD_DIR, out_name)

            with open(out_path, "wb") as out:
                for chunk in r.iter_content(chunk_size=8192):
                    out.write(chunk)

        print(f"✅ Selesai! File disimpan di: {out_path}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Gagal download: {e}")


if __name__ == "__main__":
    alive_regions = get_alive_regions()
    if not alive_regions:
        print("❌ Fatal: semua region mati, tidak bisa download apapun.")
        sys.exit(1)

    by_upload_id = collect_catalog(alive_regions)
    entries = show_catalog(by_upload_id)

    if not by_upload_id:
        sys.exit(0)

    if len(sys.argv) > 2:
        # Mode langsung: python download_client.py <REGION> [<upload_id>]
        target_id = sys.argv[2]
        copies = by_upload_id.get(target_id)
        if not copies:
            print(f"❌ upload_id '{target_id}' tidak ditemukan di region yang hidup.")
            sys.exit(1)
        download(pick_nearest_copy(copies))
    else:
        # Mode interaktif
        try:
            choice = int(input("\nPilih nomor file yang ingin didownload: "))
            _, copies = entries[choice]
            download(pick_nearest_copy(copies))
        except (ValueError, IndexError):
            print("❌ Pilihan tidak valid.")
    print()