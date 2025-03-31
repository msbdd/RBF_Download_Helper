import time
import os
import yaml
import argparse
from obspy.clients.fdsn import Client
from obspy import UTCDateTime


def load_config(config_file):
    """Load parameters from YAML configuration file."""
    with open(config_file, "r") as file:
        return yaml.safe_load(file)


def download_waveform(start_time, end_time, client, output_dir, network, station, location, channel):
    """
    Download waveform data from the FDSN server and save to a file.
    """
    try:
        print(f"Requesting data from {start_time} to {end_time}")
        st = client.get_waveforms(network=network, station=station, location=location,
                                  channel=channel, starttime=start_time, endtime=end_time)

        # File naming: station_starttime_endtime.mseed
        filename = f"RBF_{station}_{start_time.strftime('%Y%m%d_%H%M%S')}.msd"
        file_path = os.path.join(output_dir, filename)

        # Save to file
        st.write(file_path, format="MSEED")
        print(f"Data saved to {file_path}")
        return True
    except Exception as e:
        print(f"Error downloading data: {e}")
        return False


def normal_mode(config):
    """Run the downloader in continuous mode."""
    client = Client(config["server"])
    duration = config["wait"]
    retry_delay = config["retry"]
    output_dir = config["output_dir"]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    end_time = UTCDateTime()
    start_time = end_time - float(duration) * 60
    while True:
        print("\n--- Starting new download cycle ---")
        success = download_waveform(start_time, end_time, client, output_dir,
                                    config["network"], config["station"],
                                    config["location"], config["channel"])

        if success:
            print(f"Sleeping for {duration} minutes...")
            time.sleep(float(duration) * 60)
            start_time = end_time  # Move to the next time window
            end_time = UTCDateTime()
        else:
            print(f"Retrying in {retry_delay} minutes...")
            time.sleep(float(retry_delay) * 60)


def offline_mode(config):
    """Run the downloader in offline mode for a single request."""
    client = Client(config["server"])
    output_dir = config["output_dir"]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    from_time = UTCDateTime(config["offline"]["from_time"])
    to_time = UTCDateTime(config["offline"]["to_time"])
    print("\n--- Running in offline mode ---")
    success = download_waveform(from_time, to_time, client, output_dir,
                                config["network"], config["station"],
                                config["location"], config["channel"])
    if success:
        print("Offline download successful!")
    else:
        print("Offline download failed.")


def main():
    parser = argparse.ArgumentParser(description="FDSN waveform downloader with YAML configuration.")
    parser.add_argument("--config", type=str, default="config.yaml", help="Path to YAML configuration file.")
    parser.add_argument("--offline", action="store_true", help="Run in offline mode for a single download.")
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    if args.offline:
        offline_mode(config)
    else:
        normal_mode(config)


if __name__ == "__main__":
    main()
