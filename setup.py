from cx_Freeze import setup, Executable
import sys
from pathlib import Path
import obspy
import os

sys.setrecursionlimit(8000)

exe = Executable(
    script="main.py",
    base="Console",
)

obsipy_data_dir = os.path.join(
    os.path.dirname(obspy.__file__), "imaging", "data"
    )

site_packages = next(p for p in sys.path if 'site-packages' in p)
dist_info = next(Path(site_packages).glob("obspy-*.dist-info"))
setup(
    name="RBF_Download_Helper",
    version="0.1",
    description="RBF_Download_Helper",
    executables=[exe],
    options={
        "build_exe": {
            "packages": ["obspy", "yaml", "numpy", "os", "pathlib"],
            "include_files": [
                (str(dist_info), f"lib/{dist_info.name}"),
                (obsipy_data_dir, "lib/obspy/imaging/data"),
                ("example_config.txt", "example_config"),
                ("run_multiple.bat", "run_multiple.bat"),
                ("LICENSE", "LICENSE"),
            ],
            "build_exe": "build/RBF_Download_Helper"
        }
    },
)
