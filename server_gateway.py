import os
import sys
import time
from flask import Flask, request, jsonify, send_file
import redis

app = Flask(__name__)

# Path absolut berbasis lokasi script ini, supaya gateway & worker selalu
# merujuk ke folder yang sama walau dijalankan dari cwd yang berbeda-beda.
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP = os.path.join(BASE_DIR, "gateway")
os.makedirs(TEMP, exist_ok=True)

def show_tutorial():
    print("Pilih salah satu dari pilihan berikut:")
    print("  python server_gateway.py INA 5001")
    print("  python server_gateway.py SG 5002")
    print("  python server_gateway.py JPN 5003")
    print()

if len(sys.argv) < 3:
    show_tutorial()
    sys.exit(1)

REGION = sys.argv[1]
PORT = int(sys.argv[2])

# 1 folder per region -> ini adalah "gudang milik region ini sendiri".
# File hasil replikasi dari region lain juga disimpan di sini.
STORAGE_DIR = os.path.join(BASE_DIR, f"Region{REGION}")
os.makedirs(STORAGE_DIR, exist_ok=True)

db_mapping = {"INA": 0, "SG": 1, "JPN": 2}

if f"{REGION} {PORT}" not in ["INA 5001", "SG 5002", "JPN 5003"]:
    show_tutorial()
    sys.exit(1)

db_index = db_mapping.get(REGION, 0)

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True, protocol=2, db=db_index)

# Pemisah aman untuk encode "upload_id::chunk_name" di dalam antrean Redis
SEP = "::"

# ==========================
# Status Service
# ==========================
@app.route('/get-load', methods=['GET'])
def get_load():
    """Endpoint untuk Client mengecek beban server wilayah ini"""
    queue_length = redis_client.llen("task_queue")
    status = "Optimal" if queue_length < 5 else "High Load"

    return jsonify({
        "region": REGION,
        "active_queue_count": queue_length,
        "status": status
    }), 200

# ==========================
# File Service
# ==========================
@app.route('/set-metadata', methods=['POST'])
def set_metadata():
    data = request.json
    upload_id = data.get("upload_id")
    if not upload_id:
        return jsonify({"error": "upload_id wajib diisi"}), 400

    # Metadata disimpan per upload_id, bukan global, supaya upload
    # yang berjalan bersamaan (paralel) tidak saling menimpa.
    redis_client.set(f"meta:{upload_id}:total_chunks", data["total_chunks"])
    redis_client.set(f"meta:{upload_id}:original_filename", data["original_filename"])
    return jsonify({"status": "metadata_saved", "upload_id": upload_id}), 200

@app.route('/upload-chunk', methods=['POST'])
def upload_chunk():
    if 'file' not in request.files:
        return "No file part", 400

    upload_id = request.form.get('upload_id')
    if not upload_id:
        return jsonify({"error": "upload_id wajib diisi"}), 400

    file = request.files['file']
    chunk_name = file.filename

    # Nama file lokal disisipi upload_id supaya chunk dari sesi upload
    # yang berbeda tidak saling menimpa file fisik di disk.
    chunk_path = os.path.join(TEMP, f"{REGION}_{upload_id}_{chunk_name}")
    file.save(chunk_path)

    print(f"[{REGION}-Gateway] 📩 Menerima {chunk_name} (upload {upload_id}) dari Client.")
    redis_client.rpush("task_queue", f"{upload_id}{SEP}{chunk_name}")

    return jsonify({"status": "success", "region": REGION}), 200

@app.route('/receive-replica', methods=['POST'])
def receive_replica():
    """Dipanggil oleh worker region LAIN untuk menitipkan salinan file di sini,
    supaya file tetap bisa diakses walau region asalnya mati total."""
    if 'file' not in request.files:
        return "No file part", 400

    upload_id = request.form.get('upload_id')
    original_filename = request.form.get('original_filename')
    stored_filename = request.form.get('stored_filename')
    source_region = request.form.get('source_region')

    if not all([upload_id, original_filename, stored_filename, source_region]):
        return jsonify({"error": "metadata replika tidak lengkap"}), 400

    file = request.files['file']
    dest_path = os.path.join(STORAGE_DIR, stored_filename)
    file.save(dest_path)

    try:
        redis_client.hset(f"catalog:{upload_id}", mapping={
            "upload_id": upload_id,
            "region": REGION,
            "original_filename": original_filename,
            "stored_filename": stored_filename,
            "uploaded_at": time.time(),
            "is_replica": "1",
            "replicated_from": source_region,
        })
        redis_client.sadd("catalog:index", upload_id)
    except redis.exceptions.RedisError as e:
        print(f"⚠️ Gagal update katalog replika (file tetap tersimpan di disk): {e}")

    print(f"[{REGION}-Gateway] 📥 Menerima replika '{original_filename}' (upload {upload_id}) dari region {source_region}")
    return jsonify({"status": "replica_saved", "region": REGION}), 200

@app.route('/list-files', methods=['GET'])
def list_files():
    """Katalog semua file yang sudah selesai di-upload & di-merge di region ini."""
    files = []
    try:
        upload_ids = redis_client.smembers("catalog:index")
        for uid in upload_ids:
            meta = redis_client.hgetall(f"catalog:{uid}")
            if meta:
                files.append(meta)
    except redis.exceptions.RedisError as e:
        return jsonify({"error": f"redis error: {e}"}), 503

    # Urutkan terbaru dulu
    files.sort(key=lambda m: float(m.get("uploaded_at", 0)), reverse=True)
    return jsonify({"region": REGION, "files": files}), 200

def _find_file(stored_filename):
    candidate = os.path.join(STORAGE_DIR, stored_filename)
    return candidate if os.path.isfile(candidate) else None

@app.route('/download/<upload_id>', methods=['GET'])
def download_file(upload_id):
    try:
        meta = redis_client.hgetall(f"catalog:{upload_id}")
    except redis.exceptions.RedisError as e:
        return jsonify({"error": f"redis error: {e}"}), 503

    if not meta:
        return jsonify({"error": "upload_id tidak ditemukan di region ini"}), 404

    stored_filename = meta["stored_filename"]
    original_filename = meta["original_filename"]

    file_path = _find_file(stored_filename)
    if not file_path:
        return jsonify({"error": "file tercatat di katalog tapi hilang dari disk region ini"}), 404

    print(f"[{REGION}-Gateway] 📤 Serving {original_filename} (upload {upload_id})")

    # download_name membuat file yang diterima user tetap bernama asli
    # (video.txt), walau nama fisik di disk disisipi region + upload_id.
    return send_file(file_path, as_attachment=True, download_name=original_filename)

# ==========================
# Main
# ==========================
if __name__ == "__main__":
    print(f"🌍 Memulai Node Gateway untuk Wilayah: {REGION} di Port {PORT}")
    # threaded=True supaya gateway benar-benar bisa memproses banyak
    # chunk yang dikirim paralel oleh client secara bersamaan.
    app.run(host='0.0.0.0', port=PORT, debug=False, threaded=True)
    print()