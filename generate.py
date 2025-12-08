import os
import sys
import random
import time
import json


# ANSI color codes
class Colors:
    BLUE = '\033[1;34m'
    GREEN = '\033[1;32m'
    YELLOW = '\033[1;33m'
    CYAN = '\033[1;36m'
    MAGENTA = '\033[1;35m'
    BOLD = '\033[1m'
    RESET = '\033[0m'


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


def format_row_count(num_rows):
    """Convert row count to human-readable suffix for filename."""
    if num_rows >= 1_000_000_000:
        if num_rows % 1_000_000_000 == 0:
            return f"{num_rows // 1_000_000_000}b"
        else:
            return f"{num_rows / 1_000_000_000:.1f}b"
    elif num_rows >= 1_000_000:
        if num_rows % 1_000_000 == 0:
            return f"{num_rows // 1_000_000}m"
        else:
            return f"{num_rows / 1_000_000:.1f}m"
    elif num_rows >= 1_000:
        if num_rows % 1_000 == 0:
            return f"{num_rows // 1_000}k"
        else:
            return f"{num_rows / 1_000:.1f}k"
    else:
        return str(num_rows)


def main():
    folder = "data"
    if not os.path.exists(folder):
        os.makedirs(folder)

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

    # Generate filename based on row count
    row_suffix = format_row_count(num_rows)
    filename = os.path.join(folder, f"measurements-{row_suffix}.txt")

    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.CYAN}  One Billion Row Challenge - Data Generator{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")

    print(f"{Colors.BLUE}Target rows:{Colors.RESET}  {Colors.BOLD}{num_rows:,}{Colors.RESET}")
    print(f"{Colors.BLUE}Output file:{Colors.RESET}  {Colors.CYAN}{filename}{Colors.RESET}")
    print(f"{Colors.BLUE}Stations:{Colors.RESET}    {Colors.BOLD}{len(STATIONS)}{Colors.RESET} locations")
    print(f"\n{Colors.YELLOW}⏳ Generating data... (progress updates every 5M rows){Colors.RESET}\n")

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
                progress = (i + 1) / num_rows * 100
                rows_per_sec = (i + 1) / elapsed
                print(f"{Colors.MAGENTA}  ▸{Colors.RESET} {i + 1:,} rows {Colors.GREEN}({progress:.1f}%){Colors.RESET} - {elapsed:.1f}s - {Colors.CYAN}{rows_per_sec:,.0f} rows/s{Colors.RESET}")

        if buffer:
            f.writelines(buffer)

    total_time = time.time() - start_time
    file_size_mb = os.path.getsize(filename) / (1024*1024)

    print(f"\n{Colors.GREEN}✓ Generation complete!{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.BLUE}Total time:{Colors.RESET}   {Colors.BOLD}{total_time:.2f}s{Colors.RESET}")
    print(f"{Colors.BLUE}File size:{Colors.RESET}   {Colors.BOLD}{file_size_mb:.2f} MB{Colors.RESET}")
    print(f"{Colors.BLUE}Throughput:{Colors.RESET}  {Colors.BOLD}{num_rows/total_time:,.0f} rows/s{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")


if __name__ == "__main__":
    main()
