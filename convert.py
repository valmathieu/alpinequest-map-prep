import sqlite3
import os
import math
import re
import xml.etree.ElementTree as ET
from PIL import Image
import io
import multiprocessing
import time
from pathlib import Path

# --- CONFIGURATION ---
# Set to False to disable multiprocessing and run in single-thread mode
USE_MULTIPROCESSING = True  
# Define the root folder containing your region subfolders (e.g., "./cartes" or "C:/Maps")
# "." means the current folder where the script is located.
ROOT_FOLDER = "./France IGN 2019"
# ---------------------

Image.MAX_IMAGE_PIXELS = None
worker_db_conn = None

def init_worker(source_db_path):
    global worker_db_conn
    db_uri = f"file:{os.path.abspath(source_db_path)}?mode=ro"
    worker_db_conn = sqlite3.connect(db_uri, uri=True)

def process_single_tile(args):
    tx, ty, z, tl_px, tl_py, info = args
    cx_min = max(0, math.floor((tx * 256 - tl_px - 512) / 512))
    cx_max = min(info['xMax'] - 1, math.floor((tx * 256 + 256 - tl_px) / 512))
    cy_min = max(0, math.floor((ty * 256 - tl_py - 512) / 512))
    cy_max = min(info['yMax'] - 1, math.floor((ty * 256 + 256 - tl_py) / 512))
    
    out_img = None
    has_content = False
    cur = worker_db_conn.cursor()
    
    for cx in range(cx_min, cx_max + 1):
        for cy in range(cy_min, cy_max + 1):
            cur.execute("SELECT image FROM tiles WHERE z=? AND x=? AND y=?", (z, cx, cy))
            row = cur.fetchone()
            if row:
                if out_img is None:
                    out_img = Image.new('RGB', (256, 256), (255, 255, 255))
                paste_x = (tl_px + cx * 512) - (tx * 256)
                paste_y = (tl_py + cy * 512) - (ty * 256)
                try:
                    orux_img = Image.open(io.BytesIO(row[0]))
                    if orux_img.mode != 'RGB':
                        orux_img = orux_img.convert('RGB')
                    out_img.paste(orux_img, (int(paste_x), int(paste_y)))
                    has_content = True
                except Exception:
                    pass
                    
    if has_content:
        img_byte_arr = io.BytesIO()
        out_img.save(img_byte_arr, format='JPEG', quality=85)
        ty_tms = (2 ** z - 1) - ty
        return (z, tx, ty_tms, img_byte_arr.getvalue())
    return None

def parse_orux_xml(xml_file):
    with open(xml_file, 'r', encoding='utf-8') as f:
        xml_text = f.read()
    xml_text = re.sub(r'\sxmlns="[^"]+"', '', xml_text)
    root = ET.fromstring(xml_text)
    cal_data = {}
    for cal in root.findall('.//MapCalibration'):
        level = cal.get('layerLevel')
        if not level or level == "0": continue
        z = int(level)
        chunks = cal.find('.//MapChunks')
        tl = cal.find('.//CalibrationPoint[@corner="TL"]')
        if chunks is None or tl is None: continue
        cal_data[z] = {
            'xMax': int(chunks.get('xMax')), 'yMax': int(chunks.get('yMax')),
            'lat': float(tl.get('lat')), 'lon': float(tl.get('lon'))
        }
    return cal_data

def convert_map(source_db, source_xml):
    """Convert one map. Called by main loop."""
    output_file = source_db.replace('.db', '.mbtiles')
    if os.path.exists(output_file): 
        os.remove(output_file)

    print(f"\n[{time.strftime('%H:%M:%S')}] ⚙️ Starting conversion for: {os.path.basename(source_db)}")
    map_start_time = time.time()
    cal_data = parse_orux_xml(source_xml)
    
    conn_mb = sqlite3.connect(output_file)
    cur_mb = conn_mb.cursor()
    cur_mb.execute("CREATE TABLE metadata (name text, value text);")
    cur_mb.execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
    cur_mb.execute("PRAGMA synchronous=OFF")
    cur_mb.execute("PRAGMA journal_mode=MEMORY")

    pool = None
    if USE_MULTIPROCESSING:
        cores = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=cores, initializer=init_worker, initargs=(source_db,))
    else:
        init_worker(source_db)

    try:
        for z, info in cal_data.items():
            print(f"   -> Processing Zoom Level {z}...")
            
            n = 2.0 ** z
            world_px_width = 256 * n
            tl_px = ((info['lon'] + 180.0) / 360.0) * world_px_width
            lat_rad = math.radians(info['lat'])
            tl_py = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * world_px_width
            width_px = info['xMax'] * 512
            height_px = info['yMax'] * 512
            
            tx_min = int(tl_px // 256)
            tx_max = int((tl_px + width_px) // 256)
            ty_min = int(tl_py // 256)
            ty_max = int((tl_py + height_px) // 256)
            
            total_mbtiles = (tx_max - tx_min + 1) * (ty_max - ty_min + 1)
            tasks = (
                (tx, ty, z, tl_px, tl_py, info)
                for tx in range(tx_min, tx_max + 1)
                for ty in range(ty_min, ty_max + 1)
            )

            created_tiles = 0
            
            if pool:
                for result in pool.imap_unordered(process_single_tile, tasks, chunksize=100):
                    if result:
                        cur_mb.execute("INSERT INTO tiles VALUES (?, ?, ?, ?)", result)
                        created_tiles += 1
                    
                    if created_tiles % 1000 == 0:
                        elapsed = time.strftime("%H:%M:%S", time.gmtime(time.time() - map_start_time))
                        print(f"      [{elapsed}] Progress: {created_tiles}/{total_mbtiles} tiles...")
            else:
                for task in tasks:
                    result = process_single_tile(task)
                    if result:
                        cur_mb.execute("INSERT INTO tiles VALUES (?, ?, ?, ?)", result)
                        created_tiles += 1
                    
                    if created_tiles % 1000 == 0:
                        elapsed = time.strftime("%H:%M:%S", time.gmtime(time.time() - map_start_time))
                        print(f"      [{elapsed}] Progress: {created_tiles}/{total_mbtiles} tiles...")

            print(f"   ✅ Level {z} completed ({created_tiles} tiles).")

    finally:
        if pool:
            pool.close()
            pool.join()

    # Get map name from file for metadata
    map_name = os.path.splitext(os.path.basename(source_db))[0]
    cur_mb.execute("INSERT INTO metadata VALUES ('name', ?)", (map_name,))
    cur_mb.execute("INSERT INTO metadata VALUES ('format', 'jpg')")
    cur_mb.execute("INSERT INTO metadata VALUES ('type', 'overlay')")
    cur_mb.execute("INSERT INTO metadata VALUES ('version', '1.0')")
    cur_mb.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (zoom_level, tile_column, tile_row);")
    
    conn_mb.commit()
    conn_mb.close()
    
    map_elapsed = time.strftime("%H:%M:%S", time.gmtime(time.time() - map_start_time))
    print(f"[{time.strftime('%H:%M:%S')}] 🎉 Map finished in {map_elapsed} -> {output_file}")

def main():
    """Fonction principale qui scanne les dossiers et lance les conversions."""
    print("=======================================================")
    print("🗺️  OruxMaps to MBTiles Batch Converter")
    print("=======================================================")
    
    root_path = Path(ROOT_FOLDER)
    if not root_path.exists() or not root_path.is_dir():
        print(f"ERROR: The root folder '{ROOT_FOLDER}' does not exist.")
        return

    # Recherche récursive de tous les fichiers .db
    db_files = list(root_path.rglob("*.db"))
    
    if not db_files:
        print(f"No .db files found in {root_path.absolute()}")
        return

    print(f"🔍 Found {len(db_files)} potential map database(s).")
    
    maps_to_process = []
    
    # Validation des paires db/xml
    for db_path in db_files:
        # Cherche tous les fichiers xml dans le même dossier que le .db
        xml_files = list(db_path.parent.glob("*.xml"))
        if not xml_files:
            print(f"⚠️ Warning: Skipping '{db_path.name}' (no XML file found in its folder).")
            continue
            
        # On prend le premier XML trouvé (généralement il n'y en a qu'un)
        xml_path = xml_files[0]
        maps_to_process.append((str(db_path), str(xml_path)))

    if not maps_to_process:
        print("No valid db/xml pairs found to process.")
        return

    print(f"🚀 Ready to process {len(maps_to_process)} map(s).")
    
    global_start_time = time.time()
    
    # Traitement séquentiel de chaque carte
    for count, (db_file, xml_file) in enumerate(maps_to_process, 1):
        print(f"\n--- Processing Map {count}/{len(maps_to_process)} ---")
        convert_map(db_file, xml_file)
        
    global_elapsed = time.strftime("%H:%M:%S", time.gmtime(time.time() - global_start_time))
    print("\n=======================================================")
    print(f"🏆 ALL JOBS COMPLETED SUCCESSFULLY!")
    print(f"⏱️ Total time for {len(maps_to_process)} map(s): {global_elapsed}")
    print("=======================================================")

if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()