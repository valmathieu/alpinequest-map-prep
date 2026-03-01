# Orux to MBTiles Converter

A high-performance Python tool to convert local **OruxMaps (.db + .xml)** offline map databases into the open **MBTiles (.mbtiles)** standard. 

Perfect for migrating your custom maps to modern outdoor navigation apps like **AlpineQuest**, Locus Map, or QGIS.

## Why this converter? (The Grid Snapping Problem)

Standard converters often fail at high zoom levels (e.g., Zoom 16) because OruxMaps exports (like those from IGN) often use "floating" local coordinates and 512x512px tiles. Forcing these directly into an MBTiles file causes massive geographical misalignments.

This script solves this by **reading the Orux XML calibration file**. It calculates the exact global Web Mercator pixel coordinates and uses **on-the-fly image intersection** to paste your map perfectly onto the rigid 256x256 MBTiles global grid.

## Features

* **Automated Batch Processing:** Recursively scans your folders to find and convert multiple `.db`/`.xml` pairs in one go.
* **Pixel-Perfect Alignment:** Uses XML Top-Left (TL) anchor points to recalculate and snap tiles to the absolute Web Mercator grid.
* **Smart Resizing:** Automatically detects 512x512px source tiles and slices them into standard 256x256px MBTiles.
* **Multiprocessing Powered:** Utilizes 100% of your CPU cores to process thousands of tiles concurrently.
* **Live Time Tracking:** Displays elapsed time for each map and the total batch job.
* **Low RAM Footprint:** Instead of building a massive map in memory, it calculates overlaps mathematically and loads only the required source tiles per zoom level.

## Installation

1. Clone or download this repository.
2. Open your terminal (e.g., PowerShell) in the project folder.
3. Install the dependencies and set up the virtual environment automatically:

```powershell
uv add Pillow
```
