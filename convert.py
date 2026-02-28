import sqlite3
import os
import glob
import math
import re
import xml.etree.ElementTree as ET
from PIL import Image
import io

# Lève la sécurité de Pillow sur les très grandes images (au cas où)
Image.MAX_IMAGE_PIXELS = None

def parse_orux_xml(xml_file):
    """Extrait les dimensions et le point GPS d'ancrage Haut-Gauche pour chaque zoom"""
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
        print("ERREUR : Il manque le .db ou le .xml.")
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
        print(f"\n--- Traitement du Niveau de Zoom {z} ---")
        
        # 1. Calcul de la position de l'ancrage Orux sur la grille mondiale (en pixels absolus)
        n = 2.0 ** z
        world_px_width = 256 * n
        
        tl_px = ((info['lon'] + 180.0) / 360.0) * world_px_width
        lat_rad = math.radians(info['lat'])
        tl_py = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * world_px_width
        
        width_px = info['xMax'] * 512
        height_px = info['yMax'] * 512
        
        # 2. Détermination des tuiles MBTiles mondiales qui couvrent notre carte
        tx_min = int(tl_px // 256)
        tx_max = int((tl_px + width_px) // 256)
        ty_min = int(tl_py // 256)
        ty_max = int((tl_py + height_px) // 256)
        
        total_mbtiles = (tx_max - tx_min + 1) * (ty_max - ty_min + 1)
        print(f"Grille mondiale cible : {total_mbtiles} tuiles à générer.")
        
        # 3. Chargement de toutes les tuiles sources Orux de ce zoom en RAM (Rapide et léger)
        print("Chargement des tuiles sources en mémoire...")
        cur_orux.execute("SELECT x, y, image FROM tiles WHERE z=?", (z,))
        source_tiles = {(x, y): image for x, y, image in cur_orux.fetchall()}
        
        tuiles_creees = 0
        
        # 4. Génération de chaque tuile MBTiles
        for tx in range(tx_min, tx_max + 1):
            for ty in range(ty_min, ty_max + 1):
                
                # Image blanche de base 256x256
                out_img = Image.new('RGB', (256, 256), (255, 255, 255))
                has_content = False
                
                # Recherche des tuiles Orux (512x512) qui chevauchent ce carré 256x256
                # Formules d'intersection optimisées
                cx_min = max(0, math.floor((tx * 256 - tl_px - 512) / 512))
                cx_max = min(info['xMax'] - 1, math.floor((tx * 256 + 256 - tl_px) / 512))
                cy_min = max(0, math.floor((ty * 256 - tl_py - 512) / 512))
                cy_max = min(info['yMax'] - 1, math.floor((ty * 256 + 256 - tl_py) / 512))
                
                for cx in range(cx_min, cx_max + 1):
                    for cy in range(cy_min, cy_max + 1):
                        if (cx, cy) in source_tiles:
                            # Calcul de la position de collage au pixel près !
                            paste_x = (tl_px + cx * 512) - (tx * 256)
                            paste_y = (tl_py + cy * 512) - (ty * 256)
                            
                            try:
                                orux_img = Image.open(io.BytesIO(source_tiles[(cx, cy)]))
                                # Conversion en RGB (enlève l'alpha qui peut buguer sur certains formats)
                                if orux_img.mode != 'RGB':
                                    orux_img = orux_img.convert('RGB')
                                out_img.paste(orux_img, (int(paste_x), int(paste_y)))
                                has_content = True
                            except Exception as e:
                                pass
                
                if has_content:
                    # Enregistrement de l'image
                    img_byte_arr = io.BytesIO()
                    out_img.save(img_byte_arr, format='JPEG', quality=85)
                    
                    # Inversion de l'axe Y pour le format MBTiles
                    ty_tms = (2 ** z - 1) - ty
                    
                    cur_mb.execute(
                        "INSERT INTO tiles VALUES (?, ?, ?, ?)",
                        (z, tx, ty_tms, img_byte_arr.getvalue())
                    )
                    tuiles_creees += 1
                    
                    if tuiles_creees % 1000 == 0:
                        print(f"Progression : {tuiles_creees}/{total_mbtiles} tuiles générées...")

        print(f"✅ Niveau {z} terminé ({tuiles_creees} tuiles).")

    # Finalisation MBTiles
    cur_mb.execute("INSERT INTO metadata VALUES ('name', 'Carte IGN Reprojetee')")
    cur_mb.execute("INSERT INTO metadata VALUES ('format', 'jpg')")
    cur_mb.execute("INSERT INTO metadata VALUES ('type', 'overlay')")
    cur_mb.execute("INSERT INTO metadata VALUES ('version', '1.0')")
    
    cur_mb.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (zoom_level, tile_column, tile_row);")
    
    conn_mb.commit()
    conn_orux.close()
    conn_mb.close()
    print(f"\n🎉 Conversion totale terminée avec succès ! Fichier : {output_file}")

if __name__ == "__main__":
    convert_to_mbtiles()