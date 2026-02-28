import sqlite3
import os
import glob
import math
import re
import xml.etree.ElementTree as ET
from PIL import Image
import io

# Lift Pillow's security limit on very large images (just in case)
Image.MAX_IMAGE_PIXELS = None

def parse_orux_xml(xml_file):
    """Extract dimensions and the Top-Left GPS anchor point for each zoom level"""
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
            'xMax': int(chunks.get('xMax')),
            'yMax': int(chunks.get('yMax')),
            'lat': float(tl.get('lat')),
            'lon': float(tl.get('lon'))
        }
    return cal_data

def convert_to_mbtiles():
    db_files = glob.glob("*.db")
    xml_files = glob.glob("*.xml")
    
    if not db_files or not xml_files:
        print("ERROR: Missing .db or .xml file in the directory.")
        return
        
    source_db = db_files[0]
    source_xml = xml_files[0]
    output_file = source_db.replace('.db', '.mbtiles')
    
    if os.path.exists(output_file): os.remove(output_file)

    cal_data = parse_orux_xml(source_xml)
    
    conn_orux = sqlite3.connect(source_db)
    cur_orux = conn_orux.cursor()
    
    conn_mb = sqlite3.connect(output_file)
    cur_mb = conn_mb.cursor()

    cur_mb.execute("CREATE TABLE metadata (name text, value text);")
    cur_mb.execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
    cur_mb.execute("PRAGMA synchronous=OFF")
    cur_mb.execute("PRAGMA journal_mode=MEMORY")

    for z, info in cal_data.items():
        print(f"\n--- Processing Zoom Level {z} ---")
        
        # 1. Calculate the position of the Orux anchor on the global grid (in absolute pixels)
        n = 2.0 ** z
        world_px_width = 256 * n
        
        tl_px = ((info['lon'] + 180.0) / 360.0) * world_px_width
        lat_rad = math.radians(info['lat'])
        tl_py = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * world_px_width
        
        width_px = info['xMax'] * 512
        height_px = info['yMax'] * 512
        
        # 2. Determine the global MBTiles that cover our map
        tx_min = int(tl_px // 256)
        tx_max = int((tl_px + width_px) // 256)
        ty_min = int(tl_py // 256)
        ty_max = int((tl_py + height_px) // 256)
        
        total_mbtiles = (tx_max - tx_min + 1) * (ty_max - ty_min + 1)
        print(f"Target global grid: {total_mbtiles} tiles to generate.")
        
        # 3. Load all Orux source tiles for this zoom level into RAM (Fast and lightweight)
        print("Loading source tiles into memory...")
        cur_orux.execute("SELECT x, y, image FROM tiles WHERE z=?", (z,))
        source_tiles = {(x, y): image for x, y, image in cur_orux.fetchall()}
        
        created_tiles = 0
        
        # 4. Generate each MBTiles tile
        for tx in range(tx_min, tx_max + 1):
            for ty in range(ty_min, ty_max + 1):
                
                # Base white image 256x256
                out_img = Image.new('RGB', (256, 256), (255, 255, 255))
                has_content = False
                
                # Find Orux tiles (512x512) that overlap this 256x256 square
                # Optimized intersection formulas
                cx_min = max(0, math.floor((tx * 256 - tl_px - 512) / 512))
                cx_max = min(info['xMax'] - 1, math.floor((tx * 256 + 256 - tl_px) / 512))
                cy_min = max(0, math.floor((ty * 256 - tl_py - 512) / 512))
                cy_max = min(info['yMax'] - 1, math.floor((ty * 256 + 256 - tl_py) / 512))
                
                for cx in range(cx_min, cx_max + 1):
                    for cy in range(cy_min, cy_max + 1):
                        if (cx, cy) in source_tiles:
                            # Calculate pasting position pixel-perfectly!
                            paste_x = (tl_px + cx * 512) - (tx * 256)
                            paste_y = (tl_py + cy * 512) - (ty * 256)
                            
                            try:
                                orux_img = Image.open(io.BytesIO(source_tiles[(cx, cy)]))
                                # Convert to RGB (removes alpha channel which can bug on some formats)
                                if orux_img.mode != 'RGB':
                                    orux_img = orux_img.convert('RGB')
                                out_img.paste(orux_img, (int(paste_x), int(paste_y)))
                                has_content = True
                            except Exception as e:
                                pass
                
                if has_content:
                    # Save the image
                    img_byte_arr = io.BytesIO()
                    out_img.save(img_byte_arr, format='JPEG', quality=85)
                    
                    # Invert Y axis for MBTiles format
                    ty_tms = (2 ** z - 1) - ty
                    
                    cur_mb.execute(
                        "INSERT INTO tiles VALUES (?, ?, ?, ?)",
                        (z, tx, ty_tms, img_byte_arr.getvalue())
                    )
                    created_tiles += 1
                    
                    if created_tiles % 1000 == 0:
                        print(f"Progress: {created_tiles}/{total_mbtiles} tiles generated...")

        print(f"✅ Level {z} completed ({created_tiles} tiles).")

    # Finalize MBTiles
    cur_mb.execute("INSERT INTO metadata VALUES ('name', 'Reprojected Map')")
    cur_mb.execute("INSERT INTO metadata VALUES ('format', 'jpg')")
    cur_mb.execute("INSERT INTO metadata VALUES ('type', 'overlay')")
    cur_mb.execute("INSERT INTO metadata VALUES ('version', '1.0')")
    
    cur_mb.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (zoom_level, tile_column, tile_row);")
    
    conn_mb.commit()
    conn_orux.close()
    conn_mb.close()
    print(f"\n🎉 Total conversion completed successfully! File: {output_file}")

if __name__ == "__main__":
    convert_to_mbtiles()