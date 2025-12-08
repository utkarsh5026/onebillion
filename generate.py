import os
import sys
import time
import json
import tempfile
import numpy as np
import multiprocessing as mp
from multiprocessing import Value, Lock


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


def generate_rows_batch(batch_size: int) -> str:
    """Generate multiple rows at once using numpy for faster random generation."""
    station_indices = np.random.randint(0, len(STATION_NAMES), size=batch_size)

    station_names = [STATION_NAMES[idx] for idx in station_indices]
    mean_temps = STATION_TEMPS[station_indices]

    temps = np.round(np.random.normal(mean_temps, 10.0), 1)

    parts = []
    for name, temp in zip(station_names, temps):
        parts.append(name)
        parts.append(";")
        parts.append(str(temp))
        parts.append("\n")

    return "".join(parts)


def worker_generate_file(num_rows, temp_filename, progress_counter, progress_lock):
    """Worker function to generate a portion of the data in a separate file."""
    batch_size = 100_000
    rows_generated = 0

    with open(temp_filename, "w", encoding="utf-8", buffering=8192 * 1024) as f:
        while rows_generated < num_rows:
            current_batch_size = min(batch_size, num_rows - rows_generated)
            batch = generate_rows_batch(current_batch_size)

            f.write(batch)
            rows_generated += current_batch_size

            with progress_lock:
                progress_counter.value += current_batch_size

    return temp_filename


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


class FileGenerator:

    def __init__(
        self,
        num_rows,
        file_path: str,
        temp_folder: str,
        num_workers: int,
        monitor_threshold: int = 5_000_000,
    ) -> None:
        self.num_rows = num_rows
        self.file_path = file_path
        self.temp_folder = temp_folder
        self.num_workers = num_workers
        self.monitor_threshold = monitor_threshold
        self.rows_per_worker = num_rows // num_workers
        self.progress_counter = Value("i", 0)
        self.progress_lock = Lock()

    def start(self):
        """
        Start the file generation process.
        """
        self.__print_start()
        temp_files = []
        processes = []
        n = self.num_workers

        for i in range(n):
            worker_rows = self.rows_per_worker
            if i == n - 1:
                worker_rows = self.num_rows - (self.rows_per_worker * (n - 1))

            temp_filename = os.path.join(self.temp_folder, f".temp_{i}.txt")
            temp_files.append(temp_filename)

            p = mp.Process(
                target=worker_generate_file,
                args=(
                    worker_rows,
                    temp_filename,
                    self.progress_counter,
                    self.progress_lock,
                ),
            )
            p.start()
            processes.append(p)

        start_time = time.time()
        self.__monitor(processes, start_time)
        for p in processes:
            p.join()

        merge_start = time.time()
        self.__merge_temp_files(temp_files)
        self.__print_write_time(start_time)
        self.__print_final_stats(start_time, merge_start)

        return processes

    def __print_start(self):
        print(f"\n{Colors.BOLD}{'='*70}{Colors.RESET}")
        print(
            f"{Colors.CYAN}  One Billion Row Challenge - Data Generator (Multiprocessing){Colors.RESET}"
        )
        print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")

        print(
            f"{Colors.BLUE}Target rows:{Colors.RESET}  {Colors.BOLD}{self.num_rows:,}{Colors.RESET}"
        )
        print(
            f"{Colors.BLUE}Output file:{Colors.RESET}  {Colors.CYAN}{self.file_path}{Colors.RESET}"
        )
        print(
            f"{Colors.BLUE}Stations:{Colors.RESET}    {Colors.BOLD}{len(STATION_NAMES)}{Colors.RESET} locations"
        )
        print(
            f"{Colors.BLUE}Workers:{Colors.RESET}     {Colors.BOLD}{self.num_workers}{Colors.RESET} CPU cores"
        )
        print(
            f"\n{Colors.YELLOW}⏳ Generating data in parallel... (progress updates every 5M rows){Colors.RESET}\n"
        )

    def __monitor(self, processes: list[mp.Process], start_time: float):
        """
        Monitor progress of worker processes and print updates.

        Args:
            processes (list[mp.Process]): List of worker processes.
            start_time (float): Start time of the generation process.
        """
        last = 0

        while any(p.is_alive() for p in processes):
            time.sleep(0.5)

            with self.progress_lock:
                curr = self.progress_counter.value

            if curr - last >= self.monitor_threshold:
                elapsed = time.time() - start_time
                progress_pct = curr / self.num_rows * 100
                rows_per_sec = curr / elapsed if elapsed > 0 else 0
                print(
                    f"{Colors.MAGENTA}  ▸{Colors.RESET} {curr:,} rows {Colors.GREEN}({progress_pct:.1f}%){Colors.RESET} - {elapsed:.1f}s - {Colors.CYAN}{rows_per_sec:,.0f} rows/s{Colors.RESET}"
                )
                last = curr

    def __print_write_time(self, start_time: float):
        """
        Print time taken to write the final file.
        """
        elapsed = time.time() - start_time
        rows_per_sec = self.num_rows / elapsed
        print(
            f"{Colors.MAGENTA}  ▸{Colors.RESET} {self.num_rows:,} rows {Colors.GREEN}(100.0%){Colors.RESET} - {elapsed:.1f}s - {Colors.CYAN}{rows_per_sec:,.0f} rows/s{Colors.RESET}"
        )

    def __merge_temp_files(self, temp_files: list[str]):
        """
        Merge temporary files into the final output file.

        Args:
            temp_files (list[str]): List of temporary file paths.
        """
        with open(self.file_path, "wb") as outfile:
            for i, temp_file in enumerate(temp_files):
                with open(temp_file, "rb") as infile:
                    while True:
                        chunk = infile.read(1024 * 1024 * 10)  # 10MB chunks
                        if not chunk:
                            break
                        outfile.write(chunk)

                os.remove(temp_file)

    def __print_final_stats(self, start_time: float, merge_start: float):
        """
        Print final statistics after generation and merging.

        Args:
            start_time (float): Start time of the generation process.
            merge_start (float): Start time of the merging process.
        """
        generation_time = merge_start - start_time
        merge_time = time.time() - merge_start
        total_time = time.time() - start_time
        file_size_mb = os.path.getsize(self.file_path) / (1024 * 1024)

        print(f"\n{Colors.GREEN}✓ Generation complete!{Colors.RESET}")
        print(f"{Colors.BOLD}{'='*70}{Colors.RESET}")
        print(
            f"{Colors.BLUE}Generation time:{Colors.RESET} {Colors.BOLD}{generation_time:.2f}s{Colors.RESET}"
        )
        print(
            f"{Colors.BLUE}Merge time:{Colors.RESET}     {Colors.BOLD}{merge_time:.2f}s{Colors.RESET}"
        )
        print(
            f"{Colors.BLUE}Total time:{Colors.RESET}     {Colors.BOLD}{total_time:.2f}s{Colors.RESET}"
        )
        print(
            f"{Colors.BLUE}File size:{Colors.RESET}      {Colors.BOLD}{file_size_mb:.2f} MB{Colors.RESET}"
        )
        print(
            f"{Colors.BLUE}Throughput:{Colors.RESET}     {Colors.BOLD}{self.num_rows/total_time:,.0f} rows/s{Colors.RESET}"
        )
        print(f"{Colors.BOLD}{'='*70}{Colors.RESET}\n")


def get_num_rows() -> int:
    """Get number of rows to generate from command-line argument or default."""
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

    return num_rows


def main():
    folder = "data"
    if not os.path.exists(folder):
        os.makedirs(folder)

    num_rows = get_num_rows()
    row_suffix = format_row_count(num_rows)
    filename = os.path.join(folder, f"measurements-{row_suffix}.txt")

    with tempfile.TemporaryDirectory() as temp_dir:
        generator = FileGenerator(
            num_rows=num_rows,
            file_path=filename,
            temp_folder=temp_dir,
            num_workers=mp.cpu_count(),
            monitor_threshold=5_000_000,
        )
        generator.start()


if __name__ == "__main__":
    mp.freeze_support()
    main()
