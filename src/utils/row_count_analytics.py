"""Analytics script for daily row counts and visualization."""
from __future__ import annotations
import argparse
import logging
import urllib.parse
import webbrowser
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_daily_row_counts(
    gold_path: Path, 
    start_date: str | None = None, 
    end_date: str | None = None,
    days: int | None = None
) -> pd.DataFrame:
    """
    Read fact_orders partitions and count rows per date.
    
    Args:
        gold_path: Path to gold layer data
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format
        days: Optional number of most recent days to analyze (ignored if start_date/end_date provided)
        
    Returns:
        DataFrame with columns: date, row_count
    """
    fact_orders_path = gold_path / "fact_orders"
    
    if not fact_orders_path.exists():
        raise FileNotFoundError(f"fact_orders path not found: {fact_orders_path}")
    
    # Get all date partitions
    date_partitions = sorted([
        d for d in fact_orders_path.iterdir() 
        if d.is_dir() and d.name.startswith("date=")
    ])
    
    # Filter by date range if provided
    if start_date or end_date:
        filtered_partitions = []
        for partition in date_partitions:
            date_str = partition.name.split("=")[1]
            partition_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            
            if start_date:
                start = datetime.strptime(start_date, "%Y-%m-%d").date()
                if partition_date < start:
                    continue
            
            if end_date:
                end = datetime.strptime(end_date, "%Y-%m-%d").date()
                if partition_date > end:
                    continue
            
            filtered_partitions.append(partition)
        
        date_partitions = filtered_partitions
    elif days:
        # Take last N days if no date range specified
        date_partitions = date_partitions[-days:]
    
    row_counts = []
    for partition in date_partitions:
        date_str = partition.name.split("=")[1]
        parquet_file = partition / "data.parquet"
        
        if parquet_file.exists():
            df = pd.read_parquet(parquet_file)
            row_count = len(df)
            row_counts.append({
                "date": datetime.strptime(date_str, "%Y-%m-%d").date(),
                "row_count": row_count
            })
            logger.info(f"Date: {date_str}, Rows: {row_count}")
        else:
            logger.warning(f"No data.parquet found in {partition}")
    
    return pd.DataFrame(row_counts)


def calculate_average(df: pd.DataFrame) -> float:
    """Calculate average row count."""
    return df["row_count"].mean()


def plot_row_counts(df: pd.DataFrame, output_path: Path | None = None, show: bool = False) -> None:
    """
    Create a line plot of daily row counts with threshold alerting.
    
    Args:
        df: DataFrame with date and row_count columns
        output_path: Optional path to save the plot
        show: Whether to display the plot interactively (default: False)
    """
    import math
    
    plt.figure(figsize=(12, 6))
    
    # Calculate average and thresholds
    avg = df["row_count"].mean()
    floor_avg = math.floor(avg)
    threshold = floor_avg * 0.3
    
    # Plot main line in custom blue color
    plt.plot(df["date"], df["row_count"], color='#051B3F', linewidth=2, markersize=0)
    
    # Plot points with conditional coloring
    for _, row in df.iterrows():
        if row["row_count"] <= threshold:
            plt.plot(row["date"], row["row_count"], 'o', color='red', markersize=8, zorder=5)
        else:
            plt.plot(row["date"], row["row_count"], 'o', color='#051B3F', markersize=8, zorder=5)
        
        # Check if below 30% of floor(average) threshold and print warning
        if row["row_count"] < threshold:
            warning_lines = [
                "=" * 50,
                "WARNING: ORDER BELOW 30% OF 7-DAY AVERAGE",
                "=" * 50,
                f"Date: {row['date']}",
                f"Num Orders: {row['row_count']}",
                f"Threshold: {threshold:.1f} (30% of floor({avg:.2f}) = 30% of {floor_avg})",
                "=" * 50,
            ]
            warning_message = "\n".join(warning_lines)
            print("\n⚠️  " + warning_message + "\n")

            mailto_url = (
                "mailto:Joshua.Feld@ibm.com"
                "?subject=" + urllib.parse.quote("WARNING: Order Below 30% of 7-Day Average")
                + "&body=" + urllib.parse.quote(warning_message)
            )
            webbrowser.open(mailto_url)
    
    # Add average line in custom red color
    plt.axhline(y=avg, color='#FF6B6B', linestyle='--', linewidth=2, 
                label=f'Average: {avg:.1f}')
    
    # Formatting
    plt.xlabel("Date", fontsize=12, fontweight='bold')
    plt.ylabel("Row Count", fontsize=12, fontweight='bold')
    plt.title("Daily Row Counts - fact_orders", 
              fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend(fontsize=10)
    
    # Format x-axis dates
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    plt.gca().xaxis.set_major_locator(mdates.DayLocator())
    plt.xticks(rotation=45, ha='right')
    
    plt.tight_layout()
    
    if output_path:
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        logger.info(f"Plot saved to: {output_path}")
    
    if show:
        plt.show()
    else:
        plt.close()


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Analyze daily row counts from fact_orders table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Last 7 days (default)
  python src/utils/row_count_analytics.py
  
  # Last 14 days
  python src/utils/row_count_analytics.py --days 14
  
  # Specific date range
  python src/utils/row_count_analytics.py --start-date 2025-11-01 --end-date 2025-11-15
  
  # All available data
  python src/utils/row_count_analytics.py --all
        """
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of most recent days to analyze (default: 7)"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date in YYYY-MM-DD format (overrides --days)"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date in YYYY-MM-DD format (defaults to most recent date)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Analyze all available data (overrides --days)"
    )
    parser.add_argument(
        "--output",
        type=str,
        help="Custom output path for plot (default: logs/row_count_plot.png)"
    )
    
    args = parser.parse_args()
    
    # Get project root (3 levels up from this file)
    project_root = Path(__file__).parent.parent.parent
    gold_path = project_root / "data" / "gold"
    
    logger.info("=" * 60)
    logger.info("Daily Row Count Analytics - fact_orders")
    logger.info("=" * 60)
    
    # Determine date range
    if args.all:
        df = get_daily_row_counts(gold_path)
        date_range_desc = "All Available Data"
    elif args.start_date or args.end_date:
        df = get_daily_row_counts(gold_path, start_date=args.start_date, end_date=args.end_date)
        start = args.start_date or "earliest"
        end = args.end_date or "latest"
        date_range_desc = f"{start} to {end}"
    else:
        df = get_daily_row_counts(gold_path, days=args.days)
        date_range_desc = f"Last {args.days} Days"
    
    if df.empty:
        logger.error("No data found for analysis")
        return
    
    # Calculate and print average
    avg = calculate_average(df)
    num_days = len(df)
    logger.info("=" * 60)
    logger.info(f"{num_days}-Day Average Row Count: {avg:.2f}")
    logger.info("=" * 60)
    
    print("\n" + "=" * 60)
    print(f"📊 {date_range_desc.upper()}")
    print(f"Average Row Count: {avg:.2f} ({num_days} days)")
    print("=" * 60)
    print("\nDaily Breakdown:")
    for _, row in df.iterrows():
        print(f"  {row['date']}: {row['row_count']:,} rows")
    print("=" * 60 + "\n")
    
    # Create plot
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = project_root / "logs" / "row_count_plot.png"
    
    plot_row_counts(df, output_path, show=False)


if __name__ == "__main__":
    main()
