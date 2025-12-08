import os
import sys
import time
import json
import numpy as np


class Colors:
    BLUE = "\033[1;34m"
    GREEN = "\033[1;32m"
    YELLOW = "\033[1;33m"
    CYAN = "\033[1;36m"
    MAGENTA = "\033[1;35m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


def load_stations():
    stations_file = os.path.join(os.path.dirname(__file__), "stations.json")
    with open(stations_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    names = [station["name"] for station in data]
    temps = np.array([station["mean_temp"] for station in data])
    return names, temps


STATION_NAMES, STATION_TEMPS = load_stations()


def generate_rows_batch(batch_size: int) -> list[str]:
    """Generate multiple rows at once using numpy for faster random generation."""
    station_indices = np.random.randint(0, len(STATION_NAMES), size=batch_size)

    station_names = [STATION_NAMES[idx] for idx in station_indices]
    mean_temps = STATION_TEMPS[station_indices]

    temps = np.round(np.random.normal(mean_temps, 10.0), 1)

    rows = [f"{name};{temp}\n" for name, temp in zip(station_names, temps)]

    return rows


def format_row_count(num_rows):
    """Convert row count to human-readable suffix for filename."""
    if num_rows >= 1_000_000_000:
        if num_rows % 1_000_000_000 == 0:
            return f"{num_rows // 1_000_000_000}b"
        else:
            return f"{num_rows / 1_000_000_000:.1f}b"

    if num_rows >= 1_000_000:
        if num_rows % 1_000_000 == 0:
            return f"{num_rows // 1_000_000}m"
        else:
            return f"{num_rows / 1_000_000:.1f}m"

    if num_rows >= 1_000:
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

    row_suffix = format_row_count(num_rows)
    filename = os.path.join(folder, f"measurements-{row_suffix}.txt")

    print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(f"{Colors.CYAN}  One Billion Row Challenge - Data Generator{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")

    print(
        f"{Colors.BLUE}Target rows:{Colors.RESET}  {Colors.BOLD}{num_rows:,}{Colors.RESET}"
    )
    print(
        f"{Colors.BLUE}Output file:{Colors.RESET}  {Colors.CYAN}{filename}{Colors.RESET}"
    )
    print(
        f"{Colors.BLUE}Stations:{Colors.RESET}    {Colors.BOLD}{len(STATION_NAMES)}{Colors.RESET} locations"
    )
    print(
        f"\n{Colors.YELLOW}⏳ Generating data... (progress updates every 500K rows){Colors.RESET}\n"
    )

    start_time = time.time()

    batch_size = 100_000
    rows_generated = 0

    with open(filename, "w", encoding="utf-8", buffering=8192 * 1024) as f:
        while rows_generated < num_rows:
            current_batch_size = min(batch_size, num_rows - rows_generated)
            batch = generate_rows_batch(current_batch_size)

            f.writelines(batch)
            rows_generated += current_batch_size

            if rows_generated % 500_000 == 0 or rows_generated == num_rows:
                elapsed = time.time() - start_time
                progress = rows_generated / num_rows * 100
                rows_per_sec = rows_generated / elapsed
                print(
                    f"{Colors.MAGENTA}  ▸{Colors.RESET} {rows_generated:,} rows {Colors.GREEN}({progress:.1f}%){Colors.RESET} - {elapsed:.1f}s - {Colors.CYAN}{rows_per_sec:,.0f} rows/s{Colors.RESET}"
                )

    total_time = time.time() - start_time
    file_size_mb = os.path.getsize(filename) / (1024 * 1024)

    print(f"\n{Colors.GREEN}✓ Generation complete!{Colors.RESET}")
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
    print(
        f"{Colors.BLUE}Total time:{Colors.RESET}   {Colors.BOLD}{total_time:.2f}s{Colors.RESET}"
    )
    print(
        f"{Colors.BLUE}File size:{Colors.RESET}   {Colors.BOLD}{file_size_mb:.2f} MB{Colors.RESET}"
    )
    print(
        f"{Colors.BLUE}Throughput:{Colors.RESET}  {Colors.BOLD}{num_rows/total_time:,.0f} rows/s{Colors.RESET}"
    )
    print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")


if __name__ == "__main__":
    main()
