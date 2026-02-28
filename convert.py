import sqlite3
import os
import glob
import math
import re
import xml.etree.ElementTree as ET
from PIL import Image
import io

def latlon_to_xyz(lat, lon, zoom):
    """Convertit des coordonnées GPS en index de tuile Web Mercator global"""
    n = 2.0 ** zoom
    x = int((lon + 180.0) / 360.0 * n)
    lat_rad = math.radians(lat)
    y = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return x, y

def parse_orux_xml(xml_file):
    """Lit le XML Orux pour trouver l'ancrage GPS de chaque niveau de zoom"""
    with open(xml_file, 'r', encoding='utf-8') as f:
        xml_text = f.read()
    
    # Suppression des namespaces XML complexes pour faciliter la lecture
    xml_text = re.sub(r'\sxmlns="[^"]+"', '', xml_text)
    
    root = ET.fromstring(xml_text)
    calibration_data = {}
    
    print("--- Analyse du fichier XML ---")
    for cal in root.findall('.//MapCalibration'):
        layer_level = cal.get('layerLevel')
        if layer_level is None or layer_level == "0":
            continue # On ignore le niveau racine
        
        z = int(layer_level)
        
        # Trouver le point Top-Left (Haut-Gauche)
        tl_point = cal.find('.//CalibrationPoint[@corner="TL"]')
        if tl_point is None:
            continue
            
        lat = float(tl_point.get('lat'))
        lon = float(tl_point.get('lon'))
        
        # Calcul de la position de cette tuile sur la grille mondiale
        base_x, base_y_xyz = latlon_to_xyz(lat, lon, z)
        calibration_data[z] = {'base_x': base_x, 'base_y_xyz': base_y_xyz}
        
        print(f"Zoom {z:2} | Ancrage GPS trouvé -> Tuile Globale de départ X:{base_x}, Y:{base_y_xyz}")
        
    return calibration_data

def convert_to_mbtiles():
    # Recherche des fichiers
    db_files = glob.glob("*.db")
    xml_files = glob.glob("*.xml")
    
    if not db_files or not xml_files:
        print("ERREUR : Il manque le fichier .db ou le .xml dans le dossier.")
        return
        
    source_db = db_files[0]
    source_xml = xml_files[0]
    output_file = source_db.replace('.db', '.mbtiles')
    
    if os.path.exists(output_file):
        os.remove(output_file)

    # 1. Lire les calibrations
    calibrations = parse_orux_xml(source_xml)
    if not calibrations:
        print("ERREUR : Aucune donnée de calibration lue dans le XML.")
        return

    print(f"\n--- Début de la conversion : {source_db} ---")
    
    conn_orux = sqlite3.connect(source_db)
    cursor_orux = conn_orux.cursor()
    
    conn_mb = sqlite3.connect(output_file)
    cursor_mb = conn_mb.cursor()

    cursor_mb.execute("CREATE TABLE metadata (name text, value text);")
    cursor_mb.execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
    cursor_mb.execute("CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);")
    cursor_mb.execute("PRAGMA synchronous=OFF")
    cursor_mb.execute("PRAGMA journal_mode=MEMORY")

    cursor_orux.execute("SELECT z, x, y, image FROM tiles")
    rows = cursor_orux.fetchall()
    total = len(rows)
    print(f"Traitement de {total} tuiles sources (avec redécoupage si nécessaire)...")

    count = 0
    tuiles_generees = 0
    
    for z, x_local, y_local, image_data in rows:
        count += 1
        if z not in calibrations:
            continue
            
        cal = calibrations[z]
        
        # Ouverture de l'image pour analyse
        try:
            img = Image.open(io.BytesIO(image_data))
        except Exception:
            continue
            
        width, height = img.size
        
        # Calcul du multiplicateur (ex: 512px -> multiplicateur = 2)
        multiplier = width // 256
        
        # Découpage et placement
        for i in range(multiplier):
            for j in range(multiplier):
                # 1. Découpage de l'image
                left = i * 256
                upper = j * 256
                right = left + 256
                lower = upper + 256
                
                crop_img = img.crop((left, upper, right, lower))
                
                img_byte_arr = io.BytesIO()
                # On sauvegarde dans le format original (JPG ou PNG)
                fmt = img.format if img.format else 'JPEG'
                crop_img.save(img_byte_arr, format=fmt)
                tile_bytes = img_byte_arr.getvalue()
                
                # 2. Calcul des vraies coordonnées globales MBTiles
                mb_x = cal['base_x'] + (x_local * multiplier) + i
                mb_y_xyz = cal['base_y_xyz'] + (y_local * multiplier) + j
                
                # 3. Conversion en format TMS (Axe Y inversé)
                mb_y_tms = (2 ** z - 1) - mb_y_xyz
                
                # Insertion
                cursor_mb.execute(
                    "INSERT OR IGNORE INTO tiles VALUES (?, ?, ?, ?)",
                    (z, mb_x, mb_y_tms, tile_bytes)
                )
                tuiles_generees += 1

        if count % 1000 == 0:
            print(f"Progression : {count}/{total} tuiles lues...")

    cursor_mb.execute("INSERT INTO metadata VALUES ('name', 'Carte IGN Convertie')")
    cursor_mb.execute("INSERT INTO metadata VALUES ('format', 'jpg')")
    cursor_mb.execute("INSERT INTO metadata VALUES ('type', 'overlay')")
    cursor_mb.execute("INSERT INTO metadata VALUES ('version', '1.0')")
    
    conn_mb.commit()
    conn_orux.close()
    conn_mb.close()
    
    print(f"\n✅ SUCCÈS ! {tuiles_generees} tuiles standards 256px générées.")
    print(f"Fichier final : {output_file}")

if __name__ == "__main__":
    convert_to_mbtiles()