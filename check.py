import sqlite3

# Remplace par le nom exact de ton fichier
fichier = "13.otrk2.mbtiles" 

try:
    conn = sqlite3.connect(fichier)
    cur = conn.cursor()
    cur.execute("SELECT * FROM metadata")
    lignes = cur.fetchall()
    
    print("=== MÉTADONNÉES DE LA CARTE ===")
    for nom, valeur in lignes:
        print(f"{nom} : {valeur}")
        
except sqlite3.OperationalError:
    print("Erreur : Impossible d'ouvrir le fichier ou la table metadata n'existe pas.")
finally:
    if 'conn' in locals():
        conn.close()