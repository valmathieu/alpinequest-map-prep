import sqlite3
import os
import glob

def find_db_file():
    # Cherche tous les fichiers .db dans le dossier courant
    db_files = glob.glob("*.db")
    if not db_files:
        print("ERREUR : Aucun fichier .db trouvé dans ce dossier.")
        return None
    if len(db_files) > 1:
        print(f"Plusieurs fichiers .db trouvés. Utilisation de : {db_files[0]}")
    return db_files[0]

def convert_orux_to_mbtiles():
    source_file = find_db_file()
    if not source_file:
        return

    output_file = source_file.replace('.db', '.mbtiles')
    
    if os.path.exists(output_file):
        os.remove(output_file)
        print(f"Fichier existant supprimé : {output_file}")

    print(f"--- Démarrage de la conversion : {source_file} vers {output_file} ---")

    try:
        conn_orux = sqlite3.connect(source_file)
        cursor_orux = conn_orux.cursor()
    except sqlite3.Error as e:
        print(f"Erreur critique ouverture Orux: {e}")
        return

    conn_mb = sqlite3.connect(output_file)
    cursor_mb = conn_mb.cursor()

    # Création tables MBTiles
    cursor_mb.execute("CREATE TABLE metadata (name text, value text);")
    cursor_mb.execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")
    cursor_mb.execute("CREATE UNIQUE INDEX tile_index ON tiles (zoom_level, tile_column, tile_row);")
    
    # Optimisation Vitesse
    cursor_mb.execute("PRAGMA synchronous=OFF")
    cursor_mb.execute("PRAGMA journal_mode=MEMORY")

    # Détection des colonnes
    try:
        # Essai standard Orux
        cursor_orux.execute("SELECT z, x, y, image FROM tiles")
    except:
        try:
            # Essai variante (parfois 'zoom' au lieu de 'z')
            cursor_orux.execute("SELECT zoom, x, y, image FROM tiles")
        except Exception as e:
            print(f"Structure de base de données inconnue. Erreur: {e}")
            return

    rows = cursor_orux.fetchall()
    total = len(rows)
    print(f"{total} tuiles à traiter.")

    count = 0
    for z, x, y, image_data in rows:
        # Conversion XYZ (Orux) vers TMS (MBTiles)
        # Flip de l'axe Y
        y_mbtiles = (2**z - 1) - y
        
        cursor_mb.execute(
            "INSERT INTO tiles (zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)",
            (z, x, y_mbtiles, image_data)
        )
        
        count += 1
        if count % 5000 == 0:
            print(f"Progression : {count} / {total} ({(count/total)*100:.1f}%)")

    # Métadonnées basiques
    cursor_mb.execute("INSERT INTO metadata (name, value) VALUES ('name', ?)", (source_file,))
    cursor_mb.execute("INSERT INTO metadata (name, value) VALUES ('format', 'jpg')")
    cursor_mb.execute("INSERT INTO metadata (name, value) VALUES ('type', 'overlay')")
    cursor_mb.execute("INSERT INTO metadata (name, value) VALUES ('version', '1.1')")
    
    conn_mb.commit()
    conn_orux.close()
    conn_mb.close()
    
    print(f"\n✅ SUCCÈS ! Fichier créé : {output_file}")
    print("Vous pouvez maintenant copier ce fichier sur votre mobile.")

if __name__ == "__main__":
    convert_orux_to_mbtiles()