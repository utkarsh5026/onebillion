import copy
import csv
import json
import multiprocessing as mp
import sys
import tempfile
import time
from dataclasses import dataclass
from multiprocessing import Value, Lock, Manager
from multiprocessing.managers import DictProxy
from pathlib import Path
import numpy as np

STATIONS_FILE = "stations.json"
RESULTS_DIR = "results"
DATA_DIR = "data"


@dataclass
class StationResult:
    min_temp: float
    max_temp: float
    sum_temp: float
    count: int

    def update(self, other: "StationResult"):
        if other.max_temp > self.max_temp:
            self.max_temp = other.max_temp

        if other.min_temp < self.min_temp:
            self.min_temp = other.min_temp

        self.count += other.count
        self.sum_temp += other.sum_temp


class Colors:
    BLUE = "\033[1;34m"
    GREEN = "\033[1;32m"
    YELLOW = "\033[1;33m"
    CYAN = "\033[1;36m"
    MAGENTA = "\033[1;35m"
    BOLD = "\033[1m"
    RESET = "\033[0m"


def load_and_prepare_stations() -> tuple[np.ndarray, np.ndarray, np.ndarray, int]:
    """
    Load station data from JSON file and prepare np arrays for fast access.
    :return:
    """
    file = Path(__file__).parent / STATIONS_FILE
    with open(file, "r", encoding="utf-8") as f:
        data = json.load(f)

    names = [s["name"].encode("utf-8") + b";" for s in data]
    st_arr = np.array(names, dtype=object)

    mean_temps = np.array([s["mean_temp"] for s in data])

    min_temp = -2000
    max_temp = 2000

    lookup = []
    for temp in range(min_temp, max_temp + 1):
        s = f"{temp / 10.0:.1f}\n".encode("utf-8")
        lookup.append(s)

    temp_lookup = np.array(lookup, dtype=object)
    return st_arr, mean_temps, temp_lookup, -min_temp


STATION_NAMES, STATION_TEMPS, TEMP_LOOKUP, TEMP_OFFSET = load_and_prepare_stations()
POOL_SIZE = 5_000_000

GLOABAL_STATION_INDICES = np.random.randint(0, len(STATION_NAMES), size=POOL_SIZE)
GLOBAL_TEMP_INDICES = np.random.randint(0, len(TEMP_LOOKUP), size=POOL_SIZE)


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
        results_path: str | None = None,
        worker_batch_size: int = 200_000,
    ) -> None:
        self.num_rows = num_rows
        self.file_path = file_path
        self.temp_folder = temp_folder
        self.num_workers = num_workers
        self.monitor_threshold = monitor_threshold
        self.rows_per_worker = num_rows // num_workers
        self.progress_counter = Value("i", 0)
        self.progress_lock = Lock()
        self.results_path = results_path
        self.worker_batch_size = worker_batch_size
        self.station_stats: dict[str, StationResult] = {}

    def start(self):
        """
        Start the file generation process.
        """
        self.__print_start()
        temp_files = []
        processes = []
        return_dict = Manager().dict()
        n = self.num_workers

        for i in range(n):
            worker_rows = self.rows_per_worker
            if i == n - 1:
                worker_rows = self.num_rows - (self.rows_per_worker * (n - 1))

            temp_filename = str(Path(self.temp_folder) / f".temp_{i}.txt")
            temp_files.append(temp_filename)

            p = mp.Process(
                target=self.__worker,
                args=(
                    i,
                    worker_rows,
                    temp_filename,
                    return_dict,
                ),
            )
            p.start()
            processes.append(p)

        start_time = time.time()
        self.__monitor(processes, start_time)
        for p in processes:
            p.join()

        self.__aggregate_stats(return_dict)

        merge_start = time.time()
        self.__merge_temp_files(temp_files)
        self.__print_write_time(start_time)
        self.__print_final_stats(start_time, merge_start)

        if self.results_path:
            self.__write_results_csv()

        return processes

    def __worker(
        self, worker_id: int, num_rows: int, temp_file: str, return_dict: DictProxy
    ) -> None:
        rows_cnt = 0
        laps = 0
        all_station_indices, all_temp_indices = [], []
        stats: dict[str, StationResult] = {}

        with open(temp_file, "wb", buffering=8192 * 1024) as f:
            while rows_cnt < num_rows:
                batch_size = min(self.worker_batch_size, num_rows - rows_cnt)

                stations, temps, batch = self._create_worker_batch(batch_size)
                all_station_indices.append(stations)
                all_temp_indices.append(temps)

                f.write(batch)
                rows_cnt += batch_size

                with self.progress_lock:
                    self.progress_counter.value += batch_size

                laps += 1

                if laps % 10 == 0:
                    temp_stats = self.__calc_stats(
                        np.concatenate(all_station_indices),
                        np.concatenate(all_temp_indices),
                    )
                    stats.update(temp_stats)
                    all_station_indices, all_temp_indices = [], []

        return_dict[worker_id] = stats

    def _create_worker_batch(
        self, batch_size: int
    ) -> tuple[np.ndarray, np.ndarray, bytes]:
        start = np.random.randint(0, POOL_SIZE - batch_size + 1)
        end = start + batch_size

        stations = GLOABAL_STATION_INDICES[start:end]
        temps = GLOBAL_TEMP_INDICES[start:end]

        lines = STATION_NAMES[stations] + TEMP_LOOKUP[temps]
        batch = b"".join(lines.tolist())

        return stations, temps, batch

    def __calc_stats(
        self, station_indices: np.ndarray, temp_indices: np.ndarray
    ) -> dict[str, StationResult]:
        station_stats: dict[str, StationResult] = {}
        unique_stations = np.unique(station_indices)
        temps = (temp_indices - TEMP_OFFSET) / 10.0

        for station_idx in unique_stations:
            mask = station_indices == station_idx
            station_temps = temps[mask]
            station_bytes = STATION_NAMES[station_idx]
            station_name = (
                station_bytes.decode("utf-8").rstrip(";")
                if isinstance(station_bytes, bytes)
                else str(station_bytes).rstrip(";")
            )

            station_stats[station_name] = StationResult(
                max_temp=float(np.max(station_temps)),
                min_temp=float(np.min(station_temps)),
                sum_temp=float(np.sum(station_temps)),
                count=int(len(station_temps)),
            )

        return station_stats

    def __aggregate_stats(self, return_dict: DictProxy):
        """Aggregate statistics from all workers."""
        for worker_id in return_dict:
            worker_stats = return_dict[worker_id]
            for station_name, stats in worker_stats.items():
                if station_name not in self.station_stats:
                    self.station_stats[station_name] = copy.copy(stats)

                else:
                    combined = self.station_stats[station_name]
                    combined.update(stats)

    def __write_results_csv(self):
        """Write aggregated statistics to results CSV file."""
        if not self.results_path:
            return

        print(f"\n{Colors.YELLOW}>> Writing results to CSV...{Colors.RESET}")

        results_folder = Path(self.results_path).parent
        if results_folder and not results_folder.exists():
            results_folder.mkdir(parents=True, exist_ok=True)

        results = self.__prepare_csv_data()

        if len(results) == 0:
            return

        with open(self.results_path, "w", newline="", encoding="utf-8") as csvfile:
            fieldnames = list(results[0].keys())
            w = csv.DictWriter(csvfile, fieldnames=fieldnames)

            w.writeheader()
            for row in results:
                w.writerow(row)

        print(
            f"{Colors.GREEN}>> Results written to: {Colors.CYAN}{self.results_path}{Colors.RESET}"
        )
        print(
            f"{Colors.BLUE}  Total stations: {Colors.BOLD}{len(results)}{Colors.RESET}"
        )

    def __prepare_csv_data(self) -> list[dict]:
        results = []
        for station_name, stats in self.station_stats.items():
            avg = stats.sum_temp / stats.count if stats.count > 0 else 0.0
            results.append(
                {
                    "station": station_name,
                    "min": round(stats.min_temp, 1),
                    "max": round(stats.max_temp, 1),
                    "avg": round(avg, 1),
                }
            )

        results.sort(key=lambda x: x["station"])
        return results

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
        print(f"\n{Colors.YELLOW}>> Generating data in parallel...{Colors.RESET}\n")

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
                    f"{Colors.MAGENTA}  >{Colors.RESET} {curr:,} rows {Colors.GREEN}({progress_pct:.1f}%){Colors.RESET} - {elapsed:.1f}s - {Colors.CYAN}{rows_per_sec:,.0f} rows/s{Colors.RESET}"
                )
                last = curr

    def __print_write_time(self, start_time: float):
        """
        Print time taken to write the final file.
        """
        elapsed = time.time() - start_time
        rows_per_sec = self.num_rows / elapsed
        print(
            f"{Colors.MAGENTA}  >{Colors.RESET} {self.num_rows:,} rows {Colors.GREEN}(100.0%){Colors.RESET} - {elapsed:.1f}s - {Colors.CYAN}{rows_per_sec:,.0f} rows/s{Colors.RESET}"
        )

    def __merge_temp_files(self, temp_files: list[str]):
        """
        Merge temporary files into the final output file.

        Args:
            temp_files (list[str]): List of temporary file paths.
        """
        with open(self.file_path, "wb") as outfile:
            for temp_file in temp_files:
                with open(temp_file, "rb") as infile:
                    while True:
                        chunk = infile.read(1024 * 1024 * 10)  # 10MB chunks
                        if not chunk:
                            break
                        outfile.write(chunk)

                Path(temp_file).unlink()

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
        file_size_mb = Path(self.file_path).stat().st_size / (1024 * 1024)

        print(f"\n{Colors.GREEN}>> Generation complete!{Colors.RESET}")
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
    data_folder = Path(DATA_DIR)
    results_folder = Path(RESULTS_DIR)

    data_folder.mkdir(exist_ok=True)
    results_folder.mkdir(exist_ok=True)

    num_rows = get_num_rows()
    row_suffix = format_row_count(num_rows)
    filename = str(data_folder / f"measurements-{row_suffix}.txt")
    results_filename = str(results_folder / f"results-{row_suffix}.csv")

    with tempfile.TemporaryDirectory() as temp_dir:
        FileGenerator(
            num_rows=num_rows,
            file_path=filename,
            temp_folder=temp_dir,
            num_workers=mp.cpu_count(),
            monitor_threshold=5_000_000,
            results_path=results_filename,
        ).start()


if __name__ == "__main__":
    mp.freeze_support()
    main()
