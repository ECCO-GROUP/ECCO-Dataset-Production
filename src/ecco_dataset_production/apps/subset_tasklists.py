"""
CLI tool for creating subsets of ECCO tasklists for testing and quick runs.
"""
import argparse
import json
import logging
import random
from pathlib import Path

logging.basicConfig(
    format='%(levelname)-10s %(funcName)s %(asctime)s %(message)s')
log = logging.getLogger('edp')


def create_parser():
    """Set up command-line arguments for subset_tasklists.

    Returns:
        argparser.ArgumentParser instance.
    """
    parser = argparse.ArgumentParser(
        description="""Utility for creating subsets of ECCO tasklist JSON files
            for testing and quick production runs.""")

    parser.add_argument('input_path', help="""
        Path to input tasklist JSON file or directory containing multiple
        tasklist files.""")

    parser.add_argument('--output_dir', required=True, help="""
        Directory to which subset tasklist files will be written.
        Will be created if it does not exist.""")

    parser.add_argument('--mode', required=True,
        choices=['first', 'last', 'first-middle-last', 'random', 'every-nth',
                 'spread', 'bookends', 'indices', 'percentage', 'alternating'],
        help="""Subset mode:
        'first' - Select first N entries
        'last' - Select last N entries
        'first-middle-last' - Select first, middle, and last entry
        'random' - Select N random entries
        'every-nth' - Select every Nth entry (use --step to specify N)
        'spread' - Evenly distribute N entries across the entire range
        'bookends' - Select first N and last N entries
        'indices' - Select specific indices (use --indices like '0,5,10,100')
        'percentage' - Select X percent of entries (use --percent)
        'alternating' - Select every other entry""")

    parser.add_argument('-n', '--count', type=int, default=12, help="""
        Number of entries to select (for 'first', 'last', 'random', 'spread',
        and 'bookends' modes). Default: %(default)s""")

    parser.add_argument('--step', type=int, default=10, help="""
        Step size for 'every-nth' mode (select every Nth entry).
        Default: %(default)s""")

    parser.add_argument('--indices', type=str, help="""
        Comma-separated list of indices for 'indices' mode (e.g., '0,5,10,100').""")

    parser.add_argument('--percent', type=float, help="""
        Percentage of entries to select for 'percentage' mode (0-100).""")

    parser.add_argument('--pattern', default='*.json', help="""
        File pattern to match when input_path is a directory.
        Default: %(default)s""")

    parser.add_argument('--seed', type=int, help="""
        Random seed for reproducible random sampling.""")

    parser.add_argument('-l', '--log', dest='log_level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO', help="""
        Set logging level (default: %(default)s).""")

    return parser


def subset_entries(entries, mode, count=12, seed=None, step=10, indices=None, percent=None):
    """Create a subset of entries based on the specified mode.

    Args:
        entries (list): List of entries to subset.
        mode (str): Subset mode.
        count (int): Number of entries to select (for applicable modes).
        seed (int, optional): Random seed for reproducible sampling.
        step (int): Step size for 'every-nth' mode.
        indices (str): Comma-separated indices for 'indices' mode.
        percent (float): Percentage for 'percentage' mode.

    Returns:
        list: Subset of entries.
    """
    if not entries:
        log.warning("Empty entries list, returning empty subset")
        return []

    total = len(entries)

    if mode == 'first':
        subset = entries[:min(count, total)]
        log.debug(f"Selected first {len(subset)} entries")

    elif mode == 'last':
        subset = entries[-min(count, total):]
        log.debug(f"Selected last {len(subset)} entries")

    elif mode == 'first-middle-last':
        if total == 1:
            subset = entries
        elif total == 2:
            subset = [entries[0], entries[-1]]
        else:
            middle_idx = total // 2
            subset = [entries[0], entries[middle_idx], entries[-1]]
        log.debug(f"Selected first-middle-last: indices 0, {total//2}, {total-1}")

    elif mode == 'random':
        if seed is not None:
            random.seed(seed)
        sample_size = min(count, total)
        subset = random.sample(entries, sample_size)
        log.debug(f"Selected {sample_size} random entries (seed={seed})")

    elif mode == 'every-nth':
        subset = entries[::step]
        log.debug(f"Selected every {step}th entry: {len(subset)} entries")

    elif mode == 'spread':
        if count >= total:
            subset = entries
        else:
            # Evenly distribute N entries across the range
            indices_list = [int(i * (total - 1) / (count - 1)) for i in range(count)]
            subset = [entries[i] for i in indices_list]
        log.debug(f"Selected {len(subset)} entries evenly spread across range")

    elif mode == 'bookends':
        first_n = min(count, total)
        last_n = min(count, total)
        if first_n + last_n >= total:
            subset = entries
        else:
            subset = entries[:first_n] + entries[-last_n:]
        log.debug(f"Selected first {first_n} and last {last_n} entries")

    elif mode == 'indices':
        if indices is None:
            raise ValueError("'indices' mode requires --indices parameter")
        try:
            indices_list = [int(i.strip()) for i in indices.split(',')]
            # Filter out invalid indices
            valid_indices = [i for i in indices_list if 0 <= i < total]
            if len(valid_indices) < len(indices_list):
                log.warning(f"Some indices out of range (0-{total-1}), skipping them")
            subset = [entries[i] for i in valid_indices]
        except ValueError as e:
            raise ValueError(f"Invalid indices format: {e}")
        log.debug(f"Selected {len(subset)} entries at specified indices")

    elif mode == 'percentage':
        if percent is None:
            raise ValueError("'percentage' mode requires --percent parameter")
        if not 0 <= percent <= 100:
            raise ValueError(f"Percentage must be between 0 and 100, got {percent}")
        sample_size = max(1, int(total * percent / 100))
        subset = entries[:sample_size]
        log.debug(f"Selected {sample_size} entries ({percent}% of {total})")

    elif mode == 'alternating':
        subset = entries[::2]
        log.debug(f"Selected every other entry: {len(subset)} entries")

    else:
        raise ValueError(f"Unknown mode: {mode}")

    return subset


def process_tasklist_file(input_file, output_file, mode, count=12, seed=None,
                          step=10, indices=None, percent=None):
    """Process a single tasklist file and create a subset.

    Args:
        input_file (Path): Input tasklist JSON file.
        output_file (Path): Output path for subset tasklist.
        mode (str): Subset mode.
        count (int): Number of entries to select.
        seed (int, optional): Random seed for reproducible sampling.
        step (int): Step size for 'every-nth' mode.
        indices (str): Comma-separated indices for 'indices' mode.
        percent (float): Percentage for 'percentage' mode.

    Returns:
        tuple: (input_count, output_count)
    """
    log.info(f"Processing {input_file.name}...")

    try:
        with open(input_file, 'r') as f:
            data = json.load(f)

        if not isinstance(data, list):
            log.warning(f"{input_file.name} is not a JSON array, skipping")
            return (0, 0)

        input_count = len(data)
        subset = subset_entries(data, mode, count, seed, step, indices, percent)
        output_count = len(subset)

        with open(output_file, 'w') as f:
            json.dump(subset, f, indent=4)

        log.info(f"  {input_file.name}: {output_count}/{input_count} entries -> {output_file}")

        return (input_count, output_count)

    except json.JSONDecodeError as e:
        log.error(f"Failed to parse JSON in {input_file.name}: {e}")
        return (0, 0)
    except Exception as e:
        log.error(f"Error processing {input_file.name}: {e}")
        return (0, 0)


def subset_tasklists(
    input_path=None,
    output_dir=None,
    mode=None,
    count=12,
    pattern='*.json',
    seed=None,
    step=10,
    indices=None,
    percent=None,
    log_level='INFO'):
    """Create subsets of tasklist JSON files.

    Args:
        input_path (str): Path to input tasklist file or directory.
        output_dir (str): Directory to write subset tasklist files.
        mode (str): Subset mode.
        count (int): Number of entries to select (for applicable modes).
        pattern (str): File pattern to match when input_path is a directory.
        seed (int, optional): Random seed for reproducible random sampling.
        step (int): Step size for 'every-nth' mode.
        indices (str): Comma-separated indices for 'indices' mode.
        percent (float): Percentage for 'percentage' mode.
        log_level (str): Logging level.
    """
    log.setLevel(log_level)

    input_path = Path(input_path)
    output_path = Path(output_dir)

    # Create output directory
    output_path.mkdir(parents=True, exist_ok=True)
    log.info(f"Output directory: {output_path}")

    # Determine input files
    if input_path.is_file():
        input_files = [input_path]
    elif input_path.is_dir():
        input_files = sorted(input_path.glob(pattern))
        if not input_files:
            log.error(f"No files matching pattern '{pattern}' found in {input_path}")
            return
    else:
        log.error(f"Input path does not exist: {input_path}")
        return

    log.info(f"Found {len(input_files)} tasklist file(s)")

    # Build mode info string
    mode_info = f"Mode: {mode}"
    if mode in ['first', 'last', 'random', 'spread', 'bookends']:
        mode_info += f", count: {count}"
    elif mode == 'every-nth':
        mode_info += f", step: {step}"
    elif mode == 'indices':
        mode_info += f", indices: {indices}"
    elif mode == 'percentage':
        mode_info += f", percent: {percent}%"
    log.info(mode_info)

    if seed is not None:
        log.info(f"Random seed: {seed}")

    # Process each file
    total_input = 0
    total_output = 0

    for input_file in input_files:
        output_file = output_path / input_file.name
        input_count, output_count = process_tasklist_file(
            input_file, output_file, mode, count, seed, step, indices, percent)
        total_input += input_count
        total_output += output_count

    log.info("")
    log.info(f"Summary:")
    log.info(f"  Processed {len(input_files)} file(s)")
    log.info(f"  Total entries: {total_output}/{total_input}")
    log.info(f"  Output saved to: {output_path}/")


def main():
    """Main entry point for CLI."""
    parser = create_parser()
    args = parser.parse_args()

    subset_tasklists(
        input_path=args.input_path,
        output_dir=args.output_dir,
        mode=args.mode,
        count=args.count,
        pattern=args.pattern,
        seed=args.seed,
        step=args.step,
        indices=args.indices,
        percent=args.percent,
        log_level=args.log_level
    )


if __name__ == '__main__':
    main()
