import os
import sys
import random
import time
import json


# Load stations from JSON file
def load_stations():
    stations_file = os.path.join(os.path.dirname(__file__), "stations.json")
    with open(stations_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    return [(station["name"], station["mean_temp"]) for station in data]


STATIONS = load_stations()


def generate_row():
    station, mean_temp = random.choice(STATIONS)
    temp = round(random.gauss(mean_temp, 10.0), 1)
    return f"{station};{temp}\n"


def main():
    folder = "data"
    if not os.path.exists(folder):
        os.makedirs(folder)

    filename = os.path.join(folder, "measurements.txt")

    num_rows = 100_000_000
    if len(sys.argv) > 1:
        arg = sys.argv[1].lower()
        if arg == "b":
            num_rows = 1_000_000_000
        else:
            try:
                num_rows = int(arg)
            except ValueError:
                print("Error: Argument must be an integer or 'b'")
                sys.exit(1)

    print(f"Generating {num_rows:,} rows into '{filename}'...")
    print("This might take a while. Progress:")

    start_time = time.time()

    buffer_size = 10000
    buffer = []

    with open(filename, "w", encoding="utf-8") as f:
        for i in range(num_rows):
            buffer.append(generate_row())

            if len(buffer) >= buffer_size:
                f.writelines(buffer)
                buffer = []

            if (i + 1) % 5_000_000 == 0:
                elapsed = time.time() - start_time
                print(f"  - {i + 1:,} rows done ({elapsed:.2f}s)")

        if buffer:
            f.writelines(buffer)

    print(f"\nDone! Total time: {time.time() - start_time:.2f}s")
    print(f"File size: {os.path.getsize(filename) / (1024*1024):.2f} MB")


if __name__ == "__main__":
    main()
