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
# Set to True to generate missing intermediary zoom levels
GENERATE_MISSING_ZOOMS = True
# ---------------------

# Remove limit on image size to prevent DecompressionBomb errors with large map tiles
Image.MAX_IMAGE_PIXELS = None

# Global variable for the multiprocessing worker to hold its own DB connection
worker_db_conn = None
downscale_worker_conn = None


def init_worker(source_db_path):
    """
    Initializes a read-only SQLite database connection for a multiprocessing worker.

    This function is called once per worker process when the multiprocessing pool
    is created. It ensures that each process has its own isolated database connection,
    preventing threading locks and conflicts.

    Args:
        source_db_path (str): The absolute or relative path to the OruxMaps .db file.
    """
    global worker_db_conn
    # Open the database in read-only mode to improve concurrent read performance
    db_uri = f"file:{os.path.abspath(source_db_path)}?mode=ro"
    worker_db_conn = sqlite3.connect(db_uri, uri=True)


def process_single_tile(args):
    """
    Processes and constructs a single 256x256 MBTile for the Web Mercator grid.

    Since OruxMaps tiles (often 512x512) can be misaligned with the global MBTiles
    256x256 grid, this function calculates which source Orux tiles intersect with
    the target MBTile, fetches them, and pastes them at the correct offset.

    Args:
        args (tuple): A tuple containing the following elements:
            - tx (int): The target MBTile X coordinate.
            - ty (int): The target MBTile Y coordinate.
            - z (int): The zoom level.
            - tl_px (float): Absolute Web Mercator top-left X pixel coordinate of the map.
            - tl_py (float): Absolute Web Mercator top-left Y pixel coordinate of the map.
            - info (dict): Calibration metadata for this zoom level (bounds and origin).

    Returns:
        tuple: A tuple containing `(z, tx, ty_tms, image_bytes)` if the generated tile
               contains valid map data.
        None: If the generated tile is empty (no intersecting source tiles).
    """
    tx, ty, z, tl_px, tl_py, info = args

    # Calculate the range of Orux 512x512 source tiles that overlap with this 256x256 MBTile
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
                # Initialize the blank 256x256 target tile on the first successful hit
                if out_img is None:
                    out_img = Image.new('RGB', (256, 256), (255, 255, 255))

                # Calculate where to paste the source tile onto the target MBTile
                paste_x = (tl_px + cx * 512) - (tx * 256)
                paste_y = (tl_py + cy * 512) - (ty * 256)

                try:
                    orux_img = Image.open(io.BytesIO(row[0]))
                    if orux_img.mode != 'RGB':
                        orux_img = orux_img.convert('RGB')
                    out_img.paste(orux_img, (int(paste_x), int(paste_y)))
                    has_content = True
                except Exception:
                    # Silently ignore corrupted image blobs
                    pass

    if has_content:
        # Compress the composite image back to JPEG bytes
        img_byte_arr = io.BytesIO()
        out_img.save(img_byte_arr, format='JPEG', quality=85)

        # MBTiles uses TMS (Tile Map Service) Y-coordinates (origin bottom-left)
        # We must flip the Google/OSM XYZ Y-coordinate (origin top-left)
        ty_tms = (2 ** z - 1) - ty
        return (z, tx, ty_tms, img_byte_arr.getvalue())

    return None

def init_downscale_worker(mbtiles_path):
    """
    Initializes a read-only SQLite database connection for the downscaling workers.

    This function is called once per worker process to ensure each has its own
    isolated database connection, preventing threading conflicts during concurrent reads.

    Args:
        mbtiles_path (str): The absolute or relative path to the generated .mbtiles file.
    """
    global downscale_worker_conn
    db_uri = f"file:{os.path.abspath(mbtiles_path)}?mode=ro"
    downscale_worker_conn = sqlite3.connect(db_uri, uri=True, timeout=15.0)


def process_downscale_tile(args):
    """
    Worker function that assembles 4 child tiles and downscales them into 1 parent tile.

    It queries the database for the four high-resolution tiles at zoom level `z+1`
    that correspond to the target parent tile at zoom level `z`. It pastes them
    onto a 512x512 canvas and applies a high-quality Lanczos filter to downscale
    the result to a standard 256x256 MBTile.

    Args:
        args (tuple): A tuple containing:
            - z (int): The target zoom level of the parent tile.
            - px (int): The X coordinate (column) of the target parent tile.
            - py (int): The Y coordinate (row) of the target parent tile.

    Returns:
        tuple: A tuple containing `(z, px, py, image_bytes)` ready to be inserted
               into the database if successful.
        None: If none of the 4 child tiles exist (empty canvas).
    """
    z, px, py = args
    canvas = Image.new('RGB', (512, 512), (255, 255, 255))
    has_content = False

    # TMS positioning (origin is at the bottom-left of the map)
    children = {
        (2 * px, 2 * py + 1): (0, 0),  # Top-Left
        (2 * px + 1, 2 * py + 1): (256, 0),  # Top-Right
        (2 * px, 2 * py): (0, 256),  # Bottom-Left
        (2 * px + 1, 2 * py): (256, 256)  # Bottom-Right
    }

    cur = downscale_worker_conn.cursor()
    for (cx, cy), paste_pos in children.items():
        cur.execute("SELECT tile_data FROM tiles WHERE zoom_level = ? AND tile_column = ? AND tile_row = ?",
                    (z + 1, cx, cy))
        row = cur.fetchone()
        if row:
            try:
                child_img = Image.open(io.BytesIO(row[0]))
                if child_img.mode != 'RGB':
                    child_img = child_img.convert('RGB')
                canvas.paste(child_img, paste_pos)
                has_content = True
            except Exception:
                # Silently ignore corrupted image blobs
                pass

    if has_content:
        # High-quality downscaling
        final_tile = canvas.resize((256, 256), getattr(Image, 'Resampling', Image).LANCZOS)
        img_byte_arr = io.BytesIO()
        final_tile.save(img_byte_arr, format='JPEG', quality=85)
        return (z, px, py, img_byte_arr.getvalue())

    return None

def parse_orux_xml(xml_file):
    """
    Parses an OruxMaps XML calibration file to extract map bounds and coordinates.

    Reads the Top-Left (TL) anchor points and chunk limits to establish where
    the local map grid sits relative to the global world map.

    Args:
        xml_file (str): The path to the OruxMaps .xml file.

    Returns:
        dict: A dictionary mapped by zoom level (int) containing:
            - xMax (int): Maximum X chunk index.
            - yMax (int): Maximum Y chunk index.
            - lat (float): Top-Left latitude in degrees.
            - lon (float): Top-Left longitude in degrees.
    """
    with open(xml_file, 'r', encoding='utf-8') as f:
        xml_text = f.read()

    # Remove XML namespaces to simplify ElementTree parsing
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


def fill_missing_zooms(conn_mb, mbtiles_path):
    """
    Generates missing intermediate zoom levels using a multiprocessing Quadtree downscale.

    Scans the MBTiles database for gaps between the maximum and minimum available
    zoom levels. For every missing level `z`, it dispatches tasks to a multiprocessing
    pool. The workers fetch the four corresponding "parent" tiles from `z+1`, stitch
    them, and apply a high-quality Lanczos resampling to downscale them into a standard
    256x256 MBTile.

    Args:
        conn_mb (sqlite3.Connection): An active SQLite database connection to
            the target .mbtiles file (used for reading metadata and inserting new tiles).
        mbtiles_path (str): The file path to the .mbtiles database, required to
            initialize independent read-only connections for the multiprocessing workers.

    Returns:
        None: The function modifies the database in-place by inserting new tiles
        and commits the changes after completing each zoom level.
    """
    cur = conn_mb.cursor()

    # Retrieve all currently existing zoom levels in descending order
    cur.execute("SELECT DISTINCT zoom_level FROM tiles ORDER BY zoom_level DESC")
    existing_zooms = [row[0] for row in cur.fetchall()]

    if not existing_zooms:
        return

    max_z = max(existing_zooms)
    min_z = min(existing_zooms)

    # Configure the multiprocessing pool for the downscaling tasks
    pool = None
    if USE_MULTIPROCESSING:
        cores = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=cores, initializer=init_downscale_worker, initargs=(mbtiles_path,))
    else:
        init_downscale_worker(mbtiles_path)

    try:
        # Iterate downwards from max_z to min_z to fill the gaps in a cascade
        for z in range(max_z - 1, min_z - 1, -1):
            if z in existing_zooms:
                print(f"      ⏩ Zoom {z} detected (original source kept).")
                continue

            print(f"      🔨 Generating Zoom {z} (downscaling from Zoom {z + 1})...")

            # Find all unique parent tiles required for this zoom level
            # A parent tile at zoom z corresponds to coordinates (cx/2, cy/2) from zoom z+1
            cur.execute("SELECT DISTINCT tile_column / 2, tile_row / 2 FROM tiles WHERE zoom_level = ?", (z + 1,))
            parents = cur.fetchall()

            tasks = [(z, px, py) for px, py in parents]
            total = len(tasks)
            created = 0
            start_z_time = time.time()

            # Process tasks concurrently using the multiprocessing pool
            if pool:
                for result in pool.imap_unordered(process_downscale_tile, tasks, chunksize=100):
                    if result:
                        cur.execute("INSERT INTO tiles VALUES (?, ?, ?, ?)", result)
                        created += 1

                    if created % 1000 == 0:
                        elapsed = time.strftime("%M:%S", time.gmtime(time.time() - start_z_time))
                        print(f"         [{elapsed}] Progress: {created}/{total} tiles generated...")
            else:
                # Fallback to single-threaded processing if multiprocessing is disabled
                for task in tasks:
                    result = process_downscale_tile(task)
                    if result:
                        cur.execute("INSERT INTO tiles VALUES (?, ?, ?, ?)", result)
                        created += 1

                    if created % 1000 == 0:
                        elapsed = time.strftime("%M:%S", time.gmtime(time.time() - start_z_time))
                        print(f"         [{elapsed}] Progress: {created}/{total} tiles generated...")

            print(f"      ✅ Level {z} completed ({created} tiles created).")

            # IMPORTANT: Commit the database after each zoom level.
            # This ensures that the newly created level Z tiles are written to disk
            # and can be read by the workers when generating level Z-1 in the next iteration.
            conn_mb.commit()

    finally:
        # Ensure the pool is properly closed to prevent memory leaks and zombie processes
        if pool:
            pool.close()
            pool.join()


def convert_map(source_db, source_xml):
    """
    Orchestrates the conversion of a single OruxMaps map into an MBTiles file.

    This handles database creation, calculates the global Web Mercator bounding box,
    generates a task queue for all required tiles, and dispatches them to the
    multiprocessing pool.

    Args:
        source_db (str): Path to the source OruxMaps SQLite database (.db).
        source_xml (str): Path to the source OruxMaps XML calibration file (.xml).
    """
    output_file = source_db.replace('.db', '.mbtiles')
    if os.path.exists(output_file):
        os.remove(output_file)

    print(f"\n[{time.strftime('%H:%M:%S')}] ⚙️ Starting conversion for: {os.path.basename(source_db)}")
    map_start_time = time.time()

    # Retrieve calibration data to anchor the map
    cal_data = parse_orux_xml(source_xml)

    # Initialize the output MBTiles database
    conn_mb = sqlite3.connect(output_file, timeout=15.0)
    cur_mb = conn_mb.cursor()
    cur_mb.execute("CREATE TABLE metadata (name text, value text);")
    cur_mb.execute("CREATE TABLE tiles (zoom_level integer, tile_column integer, tile_row integer, tile_data blob);")

    # Performance pragmas for massive bulk inserts
    cur_mb.execute("PRAGMA synchronous=OFF")
    cur_mb.execute("PRAGMA journal_mode=WAL")

    pool = None
    if USE_MULTIPROCESSING:
        cores = multiprocessing.cpu_count()
        pool = multiprocessing.Pool(processes=cores, initializer=init_worker, initargs=(source_db,))
    else:
        init_worker(source_db)

    try:
        for z, info in cal_data.items():
            print(f"   -> Processing Zoom Level {z}...")

            # Mathematical conversion from Lat/Lon to Global Web Mercator Pixel Coordinates
            n = 2.0 ** z
            world_px_width = 256 * n
            tl_px = ((info['lon'] + 180.0) / 360.0) * world_px_width
            lat_rad = math.radians(info['lat'])
            tl_py = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * world_px_width

            # Calculate absolute map dimensions based on source 512px chunks
            width_px = info['xMax'] * 512
            height_px = info['yMax'] * 512

            # Determine the min/max 256px tile indices required for this map
            tx_min = int(tl_px // 256)
            tx_max = int((tl_px + width_px) // 256)
            ty_min = int(tl_py // 256)
            ty_max = int((tl_py + height_px) // 256)

            total_mbtiles = (tx_max - tx_min + 1) * (ty_max - ty_min + 1)

            # Generate the workload
            tasks = (
                (tx, ty, z, tl_px, tl_py, info)
                for tx in range(tx_min, tx_max + 1)
                for ty in range(ty_min, ty_max + 1)
            )

            created_tiles = 0

            # Process tasks using either the multiprocessing pool or a single thread
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
        # Ensure the pool is closed properly to prevent zombie processes
        if pool:
            pool.close()
            pool.join()

    cur_mb.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (zoom_level, tile_column, tile_row);")

    # Save original tiles before workers try to read them.
    conn_mb.commit()

    # ---- Missing levels generation ----
    if GENERATE_MISSING_ZOOMS:
        print(f"\n   🔍 Launch missing levels generator...")
        fill_missing_zooms(conn_mb, output_file)
    # ------------------------------------------------------

    # Populate MBTiles required metadata
    map_name = os.path.splitext(os.path.basename(source_db))[0]
    cur_mb.execute("INSERT INTO metadata VALUES ('name', ?)", (map_name,))
    cur_mb.execute("INSERT INTO metadata VALUES ('format', 'jpg')")
    cur_mb.execute("INSERT INTO metadata VALUES ('type', 'overlay')")
    cur_mb.execute("INSERT INTO metadata VALUES ('version', '1.0')")

    # Computing and auto adding of minzoom and maxzoom based on real file content
    cur_mb.execute("SELECT MIN(zoom_level), MAX(zoom_level) FROM tiles")
    min_z, max_z = cur_mb.fetchone()
    if min_z is not None and max_z is not None:
        cur_mb.execute("INSERT INTO metadata VALUES ('minzoom', ?)", (str(min_z),))
        cur_mb.execute("INSERT INTO metadata VALUES ('maxzoom', ?)", (str(max_z),))

    cur_mb.execute("CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (zoom_level, tile_column, tile_row);")

    conn_mb.commit()
    cur_mb.execute("PRAGMA journal_mode=DELETE")
    conn_mb.close()

    map_elapsed = time.strftime("%H:%M:%S", time.gmtime(time.time() - map_start_time))
    print(f"[{time.strftime('%H:%M:%S')}] 🎉 Map finished in {map_elapsed} -> {output_file}")


def main():
    """
    Main execution function.

    Scans the defined ROOT_FOLDER recursively for paired OruxMaps `.db` and `.xml`
    files, and triggers the conversion process for each valid map found.
    """
    print("=======================================================")
    print("🗺️  OruxMaps to MBTiles Batch Converter")
    print("=======================================================")

    root_path = Path(ROOT_FOLDER)
    if not root_path.exists() or not root_path.is_dir():
        print(f"ERROR: The root folder '{ROOT_FOLDER}' does not exist.")
        return

    # Recursively find all SQLite databases
    db_files = list(root_path.rglob("*.db"))

    if not db_files:
        print(f"No .db files found in {root_path.absolute()}")
        return

    print(f"🔍 Found {len(db_files)} potential map database(s).")

    maps_to_process = []

    # Validate that each .db has an accompanying .xml configuration file
    for db_path in db_files:
        xml_files = list(db_path.parent.glob("*.xml"))
        if not xml_files:
            print(f"⚠️ Warning: Skipping '{db_path.name}' (no XML file found in its folder).")
            continue

        xml_path = xml_files[0]
        maps_to_process.append((str(db_path), str(xml_path)))

    if not maps_to_process:
        print("No valid db/xml pairs found to process.")
        return

    print(f"🚀 Ready to process {len(maps_to_process)} map(s).")

    global_start_time = time.time()

    # Sequentially process each valid map
    for count, (db_file, xml_file) in enumerate(maps_to_process, 1):
        print(f"\n--- Processing Map {count}/{len(maps_to_process)} ---")
        convert_map(db_file, xml_file)

    global_elapsed = time.strftime("%H:%M:%S", time.gmtime(time.time() - global_start_time))
    print("\n=======================================================")
    print(f"🏆 ALL JOBS COMPLETED SUCCESSFULLY!")
    print(f"⏱️ Total time for {len(maps_to_process)} map(s): {global_elapsed}")
    print("=======================================================")


if __name__ == "__main__":
    # Required for Windows multiprocessing compatibility
    multiprocessing.freeze_support()
    main()