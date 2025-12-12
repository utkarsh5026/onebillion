#!/usr/bin/env python3
"""
Verification script to validate that results CSV files correctly represent
the aggregated statistics from the raw measurement data files.

This script:
1. Reads measurement files from data/ folder
2. Calculates min/max/avg for each station
3. Compares with the corresponding results CSV in results/ folder
4. Reports any discrepancies with detailed statistics

Usage:
    python verify.py [size]           # Verify specific size (e.g., 100k, 1m, 10m, 100m)
    python verify.py --all            # Verify all available files
    python verify.py --help           # Show help message
"""

import argparse
import csv
import multiprocessing as mp
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple


@dataclass
class StationStats:
    """Station statistics calculated from raw measurement data."""

    min_temp: float
    max_temp: float
    sum_temp: float
    count: int

    def __post_init__(self):
        """Initialize with extreme values if needed."""
        if self.min_temp == float("inf"):
            self.min_temp = 0.0
        if self.max_temp == float("-inf"):
            self.max_temp = 0.0

    @property
    def mean_temp(self) -> float:
        """Calculate mean temperature."""
        return self.sum_temp / self.count if self.count > 0 else 0.0

    def merge(self, other: "StationStats") -> None:
        """Merge another StationStats into this one."""
        self.min_temp = min(self.min_temp, other.min_temp)
        self.max_temp = max(self.max_temp, other.max_temp)
        self.sum_temp += other.sum_temp
        self.count += other.count


@dataclass
class ExpectedResult:
    """Expected result from the CSV file."""

    station: str
    min_temp: float
    max_temp: float
    mean_temp: float


@dataclass
class VerificationResult:
    """Result of verification for a single station."""

    station: str
    status: str  # "MATCH", "MIN_MISMATCH", "MAX_MISMATCH", "MEAN_MISMATCH", "MISSING"
    expected_min: float = 0.0
    actual_min: float = 0.0
    expected_max: float = 0.0
    actual_max: float = 0.0
    expected_mean: float = 0.0
    actual_mean: float = 0.0


class MeasurementVerifier:
    """Fast, robust verification of measurement data against expected results."""

    def __init__(self, tolerance: float = 0.1, workers: int = mp.cpu_count()):
        """
        Initialize verifier.

        Args:
            tolerance: Tolerance for float comparisons (degrees Celsius)
            workers: Number of worker processes (defaults to CPU count)
        """
        self.tolerance = tolerance
        self.workers = workers or mp.cpu_count()
        self.base_dir = Path(__file__).parent
        self.data_dir = self.base_dir / "data"
        self.results_dir = self.base_dir / "results"
        self.verify_dir = self.base_dir / "verify"


        self.verify_dir.mkdir(exist_ok=True)

    @staticmethod
    def parse_line(line: str) -> Tuple[str | None, float | None]:
        """
        Parse a single measurement line.

        Args:
            line: Line in format "StationName;Temperature"

        Returns:
            Tuple of (station_name, temperature)
        """
        try:
            station, temp_str = line.strip().split(";")
            temperature = float(temp_str)
            return station, temperature
        except (ValueError, IndexError) as e:
            return None, None

    def process_chunk(self, args: Tuple[str, int, int]) -> Dict[str, StationStats]:
        """
        Process a chunk of the measurement file.

        Args:
            args: Tuple of (file_path, start_byte, end_byte)

        Returns:
            Dictionary mapping station names to their statistics
        """
        file_path, start_byte, end_byte = args
        stats: Dict[str, StationStats] = {}

        with open(file_path, "r", encoding="utf-8") as f:
            # Seek to start position
            if start_byte > 0:
                f.seek(start_byte)
                # Skip partial line at the beginning (unless we're at file start)
                f.readline()

            while f.tell() < end_byte:
                line = f.readline()
                if not line:
                    break

                station, temp = self.parse_line(line)
                if station is None or temp is None:
                    continue

                if station not in stats:
                    stats[station] = StationStats(
                        min_temp=temp, max_temp=temp, sum_temp=temp, count=1
                    )
                else:
                    s = stats[station]
                    s.min_temp = min(s.min_temp, temp)
                    s.max_temp = max(s.max_temp, temp)
                    s.sum_temp += temp
                    s.count += 1

        return stats

    def calculate_statistics(self, measurement_file: Path) -> Dict[str, StationStats]:
        """
        Calculate statistics from a measurement file using multiprocessing.

        Args:
            measurement_file: Path to the measurement file

        Returns:
            Dictionary mapping station names to their statistics
        """
        file_size = measurement_file.stat().st_size
        chunk_size = max(
            file_size // self.workers, 1024 * 1024
        )  # At least 1MB per chunk

        # Create chunk arguments
        chunks = []
        for i in range(self.workers):
            start = i * chunk_size
            end = start + chunk_size if i < self.workers - 1 else file_size
            if start >= file_size:
                break
            chunks.append((str(measurement_file), start, end))

        # Process chunks in parallel
        with mp.Pool(processes=len(chunks)) as pool:
            chunk_results = pool.map(self.process_chunk, chunks)

        # Merge results from all chunks
        merged_stats: Dict[str, StationStats] = {}
        for chunk_stats in chunk_results:
            for station, stats in chunk_stats.items():
                if station not in merged_stats:
                    merged_stats[station] = stats
                else:
                    merged_stats[station].merge(stats)

        return merged_stats

    def save_calculated_results(self, actual_stats: Dict[str, StationStats], size: str) -> None:
        """
        Save calculated statistics to CSV file in verify folder.

        Args:
            actual_stats: Dictionary of calculated station statistics
            size: Dataset size (e.g., "1m", "10m", "100m")
        """
        output_file = self.verify_dir / f"results-{size}.csv"

        # Sort stations alphabetically
        sorted_stations = sorted(actual_stats.items(), key=lambda x: x[0])

        with open(output_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['station', 'min', 'max', 'avg'])

            for station, stats in sorted_stations:
                writer.writerow([
                    station,
                    f"{stats.min_temp:.1f}",
                    f"{stats.max_temp:.1f}",
                    f"{stats.mean_temp:.1f}"
                ])

        print(f"[OK] Saved calculated results to: {output_file}")

    @staticmethod
    def load_expected_results(results_file: Path) -> Dict[str, ExpectedResult]:
        """
        Load expected results from CSV file.

        Args:
            results_file: Path to the results CSV file

        Returns:
            Dictionary mapping station names to expected results
        """
        expected = {}

        with open(results_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                station = row["station"]
                # Handle both "avg" and "mean" column names
                mean_val = row.get("avg") or row.get("mean")
                if mean_val is None:
                    raise KeyError(f"CSV must have either 'avg' or 'mean' column. Found columns: {row.keys()}")
                expected[station] = ExpectedResult(
                    station=station,
                    min_temp=float(row["min"]),
                    max_temp=float(row["max"]),
                    mean_temp=float(mean_val),
                )

        return expected

    def compare_values(self, actual: float, expected: float) -> bool:
        """
        Compare two float values within tolerance.

        Args:
            actual: Actual calculated value
            expected: Expected value from CSV

        Returns:
            True if values match within tolerance
        """
        return abs(actual - expected) <= self.tolerance

    def verify_file(self, size: str) -> Tuple[bool, Dict]:
        """
        Verify a measurement file against its expected results.

        Args:
            size: Size suffix (e.g., "100k", "1m", "10m", "100m")

        Returns:
            Tuple of (success, statistics_dict)
        """
        measurement_file = self.data_dir / f"measurements-{size}.txt"
        results_file = self.results_dir / f"results-{size}.csv"

        # Check if files exist
        if not measurement_file.exists():
            return False, {"error": f"Measurement file not found: {measurement_file}"}
        if not results_file.exists():
            return False, {"error": f"Results file not found: {results_file}"}

        print(f"\n{'=' * 80}")
        print(f"Verifying: {measurement_file.name}")
        print(f"Expected:  {results_file.name}")
        print(f"{'=' * 80}")

        # Calculate statistics from measurement file
        print(
            f"Calculating statistics from measurement file (using {self.workers} workers)..."
        )
        start_time = time.time()
        actual_stats = self.calculate_statistics(measurement_file)
        calc_time = time.time() - start_time
        print(
            f"[OK] Calculated statistics for {len(actual_stats)} stations in {calc_time:.2f}s"
        )

        # Save calculated results to verify folder
        self.save_calculated_results(actual_stats, size)

        # Load expected results
        print("Loading expected results from CSV...")
        expected_results = self.load_expected_results(results_file)
        print(f"[OK] Loaded expected results for {len(expected_results)} stations")

        # Compare results
        print("\nComparing results...")
        matches = []
        mismatches = []
        missing_in_actual = []
        extra_in_actual = []

        # Check all expected stations
        for station, expected in expected_results.items():
            if station not in actual_stats:
                missing_in_actual.append(station)
                mismatches.append(
                    VerificationResult(
                        station=station,
                        status="MISSING",
                        expected_min=expected.min_temp,
                        expected_max=expected.max_temp,
                        expected_mean=expected.mean_temp,
                    )
                )
            else:
                actual = actual_stats[station]
                min_match = self.compare_values(actual.min_temp, expected.min_temp)
                max_match = self.compare_values(actual.max_temp, expected.max_temp)
                mean_match = self.compare_values(actual.mean_temp, expected.mean_temp)

                if min_match and max_match and mean_match:
                    matches.append(station)
                else:
                    status_parts = []
                    if not min_match:
                        status_parts.append("MIN")
                    if not max_match:
                        status_parts.append("MAX")
                    if not mean_match:
                        status_parts.append("MEAN")

                    mismatches.append(
                        VerificationResult(
                            station=station,
                            status=f"{'+'.join(status_parts)}_MISMATCH",
                            expected_min=expected.min_temp,
                            actual_min=actual.min_temp,
                            expected_max=expected.max_temp,
                            actual_max=actual.max_temp,
                            expected_mean=expected.mean_temp,
                            actual_mean=actual.mean_temp,
                        )
                    )

        for station in actual_stats:
            if station not in expected_results:
                extra_in_actual.append(station)

        print(f"\n{'=' * 80}")
        print("VERIFICATION RESULTS")
        print(f"{'=' * 80}")
        print(f"[+] Matched stations:       {len(matches)}/{len(expected_results)}")
        print(
            f"[-] Mismatched stations:    {len([m for m in mismatches if 'MISSING' not in m.status])}"
        )
        print(f"[-] Missing stations:       {len(missing_in_actual)}")
        print(f"[-] Extra stations:         {len(extra_in_actual)}")
        print(f"  Total expected:         {len(expected_results)}")
        print(f"  Total actual:           {len(actual_stats)}")
        print(f"  Tolerance:              ±{self.tolerance}°C")
        print(f"  Verification time:      {calc_time:.2f}s")

        # Show detailed mismatches if any
        if mismatches:
            print(f"\n{'=' * 80}")
            print("DETAILED MISMATCHES")
            print(f"{'=' * 80}")
            for result in mismatches[:20]:  # Show first 20 mismatches
                print(f"\nStation: {result.station}")
                print(f"  Status: {result.status}")
                if "MIN" in result.status:
                    print(
                        f"  Min:  expected={result.expected_min:.1f}, actual={result.actual_min:.1f}, diff={abs(result.expected_min - result.actual_min):.1f}"
                    )
                if "MAX" in result.status:
                    print(
                        f"  Max:  expected={result.expected_max:.1f}, actual={result.actual_max:.1f}, diff={abs(result.expected_max - result.actual_max):.1f}"
                    )
                if "MEAN" in result.status:
                    print(
                        f"  Mean: expected={result.expected_mean:.1f}, actual={result.actual_mean:.1f}, diff={abs(result.expected_mean - result.actual_mean):.1f}"
                    )

            if len(mismatches) > 20:
                print(f"\n... and {len(mismatches) - 20} more mismatches")

        if extra_in_actual:
            print(
                f"\nExtra stations (in actual but not in expected): {extra_in_actual[:10]}"
            )
            if len(extra_in_actual) > 10:
                print(f"... and {len(extra_in_actual) - 10} more")

        # Determine success
        success = len(mismatches) == 0 and len(extra_in_actual) == 0

        if success:
            print(f"\n{'=' * 80}")
            print("[PASS] VERIFICATION PASSED - All results match expected values!")
            print(f"{'=' * 80}")
        else:
            print(f"\n{'=' * 80}")
            print("[FAIL] VERIFICATION FAILED - Discrepancies found!")
            print(f"{'=' * 80}")

        stats = {
            "success": success,
            "matched": len(matches),
            "mismatched": len([m for m in mismatches if "MISSING" not in m.status]),
            "missing": len(missing_in_actual),
            "extra": len(extra_in_actual),
            "total_expected": len(expected_results),
            "total_actual": len(actual_stats),
            "verification_time": calc_time,
        }

        return success, stats


def main():
    """Main entry point for verification script."""
    parser = argparse.ArgumentParser(
        description="Verify measurement data against expected results",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python verify.py 100k          # Verify 100k dataset
  python verify.py 1m            # Verify 1m dataset
  python verify.py --all         # Verify all available datasets
  python verify.py --tolerance 0.5  # Use custom tolerance
        """,
    )
    parser.add_argument(
        "size", nargs="?", help="Dataset size to verify (e.g., 100k, 1m, 10m, 100m)"
    )
    parser.add_argument(
        "--all", action="store_true", help="Verify all available datasets"
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.1,
        help="Tolerance for float comparisons in degrees Celsius (default: 0.1)",
    )
    parser.add_argument(
        "--workers", type=int, help="Number of worker processes (default: CPU count)"
    )

    args = parser.parse_args()

    # Validate arguments
    if not args.all and not args.size:
        parser.print_help()
        sys.exit(1)

    # Create verifier
    verifier = MeasurementVerifier(tolerance=args.tolerance, workers=args.workers)

    # Determine which files to verify
    if args.all:
        sizes = ["100k", "1m", "10m", "100m"]
        # Only verify files that exist
        sizes = [
            s for s in sizes if (verifier.data_dir / f"measurements-{s}.txt").exists()
        ]
    else:
        sizes = [args.size]

    # Verify each file
    results = {}
    overall_success = True

    for size in sizes:
        success, stats = verifier.verify_file(size)
        results[size] = stats
        overall_success = overall_success and success

    # Print summary if multiple files
    if len(sizes) > 1:
        print(f"\n{'=' * 80}")
        print("OVERALL SUMMARY")
        print(f"{'=' * 80}")
        for size, stats in results.items():
            if "error" in stats:
                print(f"{size:10s}: ERROR - {stats['error']}")
            else:
                status = "[PASS]" if stats["success"] else "[FAIL]"
                print(
                    f"{size:10s}: {status} - {stats['matched']}/{stats['total_expected']} matched"
                )

        if overall_success:
            print(f"\n[PASS] ALL VERIFICATIONS PASSED")
            sys.exit(0)
        else:
            print(f"\n[FAIL] SOME VERIFICATIONS FAILED")
            sys.exit(1)
    else:
        sys.exit(0 if overall_success else 1)


if __name__ == "__main__":
    main()