# alpinequest-map-prep

Quick and simple Python script to convert OruxMaps maps (SQLite .db) to the standard MBTiles format, optimized for AlpineQuest.

## Features

- **Automatic detection** of the `.db` file in the folder.

- **Fast conversion** using in-memory SQLite optimization.

- **Y-axis inversion** (converts Orux's XYZ format to the MBTiles TMS format).

- **Zero external dependencies** (uses only the Python standard library).

## Usage

1. Place your OruxMaps map file (`map_name.db`) in the same folder as the script.

2. Open your terminal (e.g., PowerShell) in that folder.

3. Run the script via `uv`:

PowerShell

```powershell
uv run convert.py
```
