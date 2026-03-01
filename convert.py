import sqlite3
import os
import glob
import math
import re
import xml.etree.ElementTree as ET
from PIL import Image
import io
import multiprocessing
import time

# --- CONFIGURATION ---
# Set to False to disable multiprocessing and run in single-thread mode
USE_MULTIPROCESSING = True  
# ---------------------

# Lift Pillow's security limit on very large images (just in case)
Image.MAX_IMAGE_PIXELS = None

# Global variable specific to EACH worker process
worker_db_conn = None

def init_worker(source_db_path):
    """
    Initializes the SQLite connection for each CPU core.
    Uses 'ro' (Read-Only) mode to allow massive simultaneous reads.
    In single-process mode, this initializes the main thread's connection.
    """
    global worker_db_conn
    db_uri = f"file:{os.path.abspath(source_db_path)}?mode=ro"
    worker_db_conn = sqlite3.connect(db_uri, uri=True)

def process_single_tile(args):
    """
    The function executed in parallel by each CPU core (or sequentially if disabled).
    """
    tx, ty, z, tl_px, tl_py, info = args
    
    # Calculate intersections (which Orux source tiles touch this MBTiles square)
    cx_min = max(0, math.floor((tx * 256 - tl_px - 512) / 512))
    cx_max = min(info['xMax'] - 1, math.floor((tx * 256 + 256 - tl_px) / 512))
    cy_min = max(0, math.floor((ty * 256 - tl_py - 512) / 512))
    cy_max = min(info['yMax'] - 1, math.floor((ty * 256 + 256 - tl_py) / 512))
    
    out_img = None
    has_content = False
    cur = worker_db_conn.cursor()
    
    for cx in range(cx_min, cx_max + 1):
        for cy in range(cy_min, cy_max + 1):
            # Fetch the image directly from the database
            cur.execute("SELECT image FROM tiles WHERE z=? AND x=? AND y=?", (z, cx, cy))
            row = cur.fetchone()
            
            if row:
                if out_img is None:
                    # Create the white canvas only if a tile is found
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
        # JPEG compression in RAM
        img_byte_arr = io.BytesIO()
        out_img.save(img_byte_arr, format='JPEG', quality=85)
        
        # Invert Y axis (TMS standard)
        ty_tms = (2 ** z - 1) - ty
        
        # Return the result back to the Main Process
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

def convert_to_mbtiles():
    db_files = glob.glob("*.db")
    xml_files = glob.glob("*.xml")
    
    if not db_files or not xml_files:
        print("ERROR: Missing .db or .xml file.")
        return
        
    source_db = db_files[0]
    source_xml = xml_files[0]
    output_file = source_db.replace('.db', '.mbtiles')
    
    if os.path.exists(output_file): os.remove(output_file)

    cal_data = parse_orux_xml(source_xml)
    
    # MBTiles creation (Main Process)
    conn_mb = sqlite3.connect(output_file)
    cur_mb = conn_mb.cursor()
    cur_mb.execute("CREATE TABLE metadata (name text, value text);")
    cur_mb.execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
    cur_mb.execute("PRAGMA synchronous=OFF")
    cur_mb.execute("PRAGMA journal_mode=MEMORY")

    pool = None
    if USE_MULTIPROCESSING:
        cores = multiprocessing.cpu_count()
        print(f"🚀 Starting in Multiprocessing mode ({cores} cores)...")
        pool = multiprocessing.Pool(processes=cores, initializer=init_worker, initargs=(source_db,))
    else:
        print("🐌 Starting in Single-process mode (Multiprocessing disabled)...")
        init_worker(source_db)
        
    # Init time measuring
    global_start_time = time.time()

    try:
        for z, info in cal_data.items():
            print(f"\n--- Processing Zoom Level {z} ---")
            
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
            print(f"Target global grid: {total_mbtiles} tiles to generate.")

            # Task list generator (uses almost 0 RAM)
            tasks = (
                (tx, ty, z, tl_px, tl_py, info)
                for tx in range(tx_min, tx_max + 1)
                for ty in range(ty_min, ty_max + 1)
            )

            created_tiles = 0
            
            if pool:
                # Execution distributed across CPU cores
                for result in pool.imap_unordered(process_single_tile, tasks, chunksize=100):
                    if result:
                        cur_mb.execute("INSERT INTO tiles VALUES (?, ?, ?, ?)", result)
                        created_tiles += 1
                    
                    if created_tiles % 1000 == 0:
                        # Compute and format time
                        elapsed = time.time() - global_start_time
                        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
                        
                        print(f"[{elapsed_str}] Progress: {created_tiles}/{total_mbtiles} tiles generated...")
            else:
                # Execution sequentially on the main thread
                for task in tasks:
                    result = process_single_tile(task)
                    if result:
                        cur_mb.execute("INSERT INTO tiles VALUES (?, ?, ?, ?)", result)
                        created_tiles += 1
                    
                    if created_tiles % 1000 == 0:
                        # Compute and format time
                        elapsed = time.time() - global_start_time
                        elapsed_str = time.strftime("%H:%M:%S", time.gmtime(elapsed))
                        
                        print(f"[{elapsed_str}] Progress: {created_tiles}/{total_mbtiles} tiles generated...")

            print(f"✅ Level {z} completed ({created_tiles} tiles).")

    finally:
        # Ensures the pool is properly closed even if an error occurs
        if pool:
            pool.close()
            pool.join()

    cur_mb.execute("INSERT INTO metadata VALUES ('name', 'Reprojected Map')")
    cur_mb.execute("INSERT INTO metadata VALUES ('format', 'jpg')")
    cur_mb.execute("INSERT INTO metadata VALUES ('type', 'overlay')")
    cur_mb.execute("INSERT INTO metadata VALUES ('version', '1.0')")
    cur_mb.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (zoom_level, tile_column, tile_row);")
    
    conn_mb.commit()
    conn_mb.close()
    
    # End time measuring
    total_elapsed = time.time() - global_start_time
    total_str = time.strftime("%H:%M:%S", time.gmtime(total_elapsed))
    
    print(f"\n🎉 Total conversion completed successfully in {total_str}! File: {output_file}")

# MANDATORY security on Windows for Multiprocessing
if __name__ == "__main__":
    multiprocessing.freeze_support()
    convert_to_mbtiles()