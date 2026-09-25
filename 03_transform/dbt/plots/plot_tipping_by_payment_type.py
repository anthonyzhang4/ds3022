"""Plot tipping behavior by payment type from mart_tipping_by_payment_type.

Run `dbt run --select +mart_tipping_by_payment_type` first so the table
exists in nyc_taxi.duckdb, then:

    python plot_tipping_by_payment_type.py
"""

from pathlib import Path

import duckdb
import matplotlib.pyplot as plt
import seaborn as sns

DB_PATH = Path(__file__).parent / "../nyc_taxi.duckdb"
OUTPUT_PATH = Path(__file__).parent / "tipping_by_payment_type.png"

# credit_card and cash dominate trip volume in this dataset; the rest
# (no_charge/dispute/unknown/voided_trip) are rare edge cases whose tiny
# sample sizes would just add noise to the comparison
FOCUS_TYPES = ["credit_card", "cash"]


def main():
    con = duckdb.connect(str(DB_PATH), read_only=True)
    df = con.execute(
        """
        select payment_type_label, trip_count, avg_tip_pct, pct_zero_tip
        from mart_tipping_by_payment_type
        where payment_type_label in ('credit_card', 'cash')
        order by trip_count desc
        """
    ).df()
    con.close()

    sns.set_theme(style="whitegrid")
    fig, axes = plt.subplots(1, 2, figsize=(11, 6))

    sns.barplot(data=df, x="payment_type_label", y="avg_tip_pct", color="steelblue", ax=axes[0])
    axes[0].set_title("Average Tip % of Fare")
    axes[0].set_xlabel("Payment Type")
    axes[0].set_ylabel("Avg Tip (% of fare)")
    for i, v in enumerate(df["avg_tip_pct"]):
        axes[0].text(i, v, f"{v:.1f}%", ha="center", va="bottom")

    sns.barplot(data=df, x="payment_type_label", y="pct_zero_tip", color="darkorange", ax=axes[1])
    axes[1].set_title("Trips Recorded With Zero Tip")
    axes[1].set_xlabel("Payment Type")
    axes[1].set_ylabel("% of Trips With $0 Tip")
    for i, v in enumerate(df["pct_zero_tip"]):
        axes[1].text(i, v, f"{v:.1f}%", ha="center", va="bottom")

    fig.suptitle("NYC Yellow Taxi: Tipping by Payment Type (2025)")
    fig.tight_layout()
    fig.savefig(OUTPUT_PATH, dpi=150)
    print(f"Saved plot to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
