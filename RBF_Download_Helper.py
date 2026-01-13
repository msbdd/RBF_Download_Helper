import time
import os
import yaml
import argparse
from obspy.clients.fdsn import Client
from obspy import UTCDateTime
from obspy.clients.fdsn.header import FDSNNoDataException


def load_config(config_file):
    """
    Load parameters from YAML configuration file
    """
    with open(config_file, "r") as file:
        return yaml.safe_load(file)


def download_waveform(start_time, end_time, client, output_dir,
                      network, station, location, channel,
                      optional_id=None):
    """
    Download waveform data from the FDSN server and save to a file
    """
    try:
        print(f"Requesting data from {start_time} to {end_time}")
        st = client.get_waveforms(network=network, station=station,
                                  location=location, channel=channel,
                                  starttime=start_time, endtime=end_time)

        if not optional_id:
            filename = (
                f"RBF_{station}_"
                f"{start_time.strftime('%Y%m%d_%H%M%S')}.msd"
            )
        else:
            filename = (
                f"RBF_{optional_id}_"
                f"{start_time.strftime('%Y%m%d_%H%M%S')}.msd"
            )

        file_path = os.path.join(output_dir, filename)

        # Save to file
        # st.merge(method = 1, fill_value = 0)
        st.write(file_path, format="MSEED")
        print(f"Data saved to {file_path}")
        return 0
    except FDSNNoDataException:
        print("No data available for the request.")
        return 204
    except Exception as e:
        print(f"Error downloading data: {e}")
        return -1


def normal_mode(config):
    """
    Run the downloader in continuous mode
    """
    client = Client(config["server"])
    duration = config["duration"]
    retry_delay = config["retry"]
    output_dir = config["output_dir"]
    # Buffer time (in seconds) to wait after the time window is complete
    # to ensure data is available on the server
    buffer_seconds = float(config.get("buffer", 30))
    if buffer_seconds < 0:
        print("Warning: buffer cannot be negative, using 0")
        buffer_seconds = 0

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    optional_id = config.get("optional_id")
    save_file_path = config["save_file"]
    try:
        with open(save_file_path, "r") as save_file:
            line = save_file.readline()
        start_time = UTCDateTime(line)
        end_time = UTCDateTime.now()

    except Exception:
        print("Cannot process the provided save file,"
              " continuing from the current time")
        end_time = UTCDateTime.now()
        start_time = end_time - float(duration) * 60

    while True:
        if ((end_time-start_time) >= float(duration) * 60):
            print("\n--- Starting new download cycle ---")
            end_time = start_time + float(duration) * 60
            result = download_waveform(
                start_time, end_time, client,
                output_dir, config["network"],
                config["station"], config["location"],
                config["channel"], optional_id
            )

            if result == 0 or result == 204:
                if result == 204:
                    print("Forwarding the time window as "
                          "there is no data available...")
                with open(save_file_path, "w") as save_file:
                    timestring = end_time.isoformat()
                    print(timestring, file=save_file)
                start_time = end_time
                end_time = UTCDateTime.now()
                continue
            else:
                print(f"Retrying in {retry_delay} minutes...")
                time.sleep(float(retry_delay) * 60)
                continue

        else:
            target_time = start_time + float(duration) * 60
            # Add buffer time to ensure data is available on the server
            remaining_seconds = (target_time + buffer_seconds) - UTCDateTime.now()

            if remaining_seconds > 0:
                print(f"Window not full. Sleeping for "
                      f"{remaining_seconds/60:.2f} minutes "
                      f"(includes {buffer_seconds}s buffer)...")
                time.sleep(remaining_seconds)

            end_time = UTCDateTime.now()


def offline_mode(config):
    """
    Run the downloader in offline mode for a single request
    """
    client = Client(config["server"])
    output_dir = config["output_dir"]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    optional_id = config.get("optional_id")

    from_time = UTCDateTime(config["offline"]["from_time"])
    to_time = UTCDateTime(config["offline"]["to_time"])
    print("\n--- Running in offline mode ---")
    result = download_waveform(
        from_time, to_time, client, output_dir,
        config["network"], config["station"],
        config["location"], config["channel"], optional_id
    )
    if result == 0:
        print("Offline download successful")
    elif result == 204:
        print("Offline request returned no data")
    else:
        print("Offline download failed")


def main():
    parser = argparse.ArgumentParser(description="RBF Download Helper")
    parser.add_argument("--config", type=str, default="config.yaml",
                        help="Path to configuration file")
    parser.add_argument("--offline", action="store_true",
                        help="Run in offline mode for a single download")
    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    if args.offline:
        offline_mode(config)
    else:
        normal_mode(config)


if __name__ == "__main__":
    main()
