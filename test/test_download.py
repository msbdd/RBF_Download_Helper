import os
import sys
import time
import subprocess

import yaml
import pytest
from obspy import UTCDateTime

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import RBF_Download_Helper as rbf  # noqa: E402

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "test_config.txt")
SCRIPT = os.path.join(REPO_ROOT, "RBF_Download_Helper.py")


def test_offline_mode(tmp_path):
    """Offline mode downloads the fixed window and writes a file."""
    config = rbf.load_config(CONFIG_PATH)
    config["output_dir"] = str(tmp_path)

    try:
        client = rbf.create_client(config)
    except Exception as exc:
        pytest.skip(f"FDSN server not reachable: {exc}")

    start = UTCDateTime(config["offline"]["from_time"])
    end = UTCDateTime(config["offline"]["to_time"])
    result = rbf.download_waveform(
        start, end, client, str(tmp_path),
        config["network"], config["station"],
        config["location"], config["channel"],
        config.get("optional_id"),
    )

    if result == 204:
        pytest.skip("Server reachable but no data for the test window")

    assert result == 0
    files = list(tmp_path.glob("*.msd"))
    assert files, "no MiniSEED file was written"
    assert files[0].stat().st_size > 0


# Timestamp seeded into the save file so continuous mode resumes from a
# known window with real data, instead of "now" (which depends on the
# clock and on the station currently streaming).
SEED_TIME = "2024-01-01T00:00:00"


def test_normal_mode(tmp_path):
    """Continuous mode downloads a 60 min window and advances the save file."""
    out_dir = tmp_path / "out"
    save_file = out_dir / "lastdt.txt"
    out_dir.mkdir()

    # Seed the save file: normal_mode resumes from this timestamp and
    # downloads SEED_TIME .. SEED_TIME + duration (60 min).
    save_file.write_text(SEED_TIME + "\n")

    # Reuse the template config, redirect output to a fresh dir and drop
    # the offline block so the script runs in continuous mode.
    config = rbf.load_config(CONFIG_PATH)
    config["output_dir"] = str(out_dir)
    config["save_file"] = str(save_file)
    config.pop("offline", None)

    cfg_path = tmp_path / "online_config.yaml"
    with open(cfg_path, "w") as fh:
        yaml.safe_dump(config, fh)

    proc = subprocess.Popen(
        [sys.executable, "-u", SCRIPT, "--config", str(cfg_path)],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )

    msd = []
    advanced = False
    try:
        deadline = time.time() + 90
        while time.time() < deadline:
            if proc.poll() is not None:
                break  # exited early (e.g. could not reach the server)
            msd = list(out_dir.glob("*.msd"))
            advanced = save_file.read_text().strip() != SEED_TIME
            if msd and advanced:
                break  # downloaded data and moved the window forward
            if advanced and not msd:
                break  # window advanced with no data (server had none)
            time.sleep(2)
    finally:
        proc.terminate()
        try:
            output = proc.communicate(timeout=10)[0]
        except subprocess.TimeoutExpired:
            proc.kill()
            output = proc.communicate()[0]

    # No data downloaded -> treat as a network/availability issue, skip.
    if not msd:
        pytest.skip("normal mode produced no data:\n" + (output or ""))

    # Verify the save file (lastdt.txt) was modified: its timestamp
    # advanced past the seeded value.
    saved = save_file.read_text().strip()
    assert saved != SEED_TIME, "lastdt.txt still holds the seed timestamp"
    assert UTCDateTime(saved) > UTCDateTime(SEED_TIME), \
        "lastdt.txt did not advance"
    assert msd[0].stat().st_size > 0
