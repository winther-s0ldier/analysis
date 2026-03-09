"""
Chart Generator Tool — creates matplotlib/plotly charts for various analysis types.
Used by the DAG Builder Agent.
"""
import os
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np
from typing import Optional


def create_chart(
    csv_path: str,
    metric_name: str,
    analysis_type: str,
    required_columns: list,
    output_dir: str,
    chart_format: str = "png",
) -> dict:
    """
    Generate a chart based on the analysis type and save it.
    
    Args:
        csv_path: Path to the CSV data file.
        metric_name: Name of the metric for the chart title.
        analysis_type: One of distribution, trend, correlation, group_comparison, anomaly, frequency, regression.
        required_columns: Columns needed for this chart.
        output_dir: Directory to save charts to.
        chart_format: "png" or "html".
    
    Returns:
        dict with status, file_path, and chart_type.
    """
    try:
        df = pd.read_csv(csv_path)
    except Exception as e:
        return {"status": "error", "error": f"Failed to read CSV: {str(e)}"}

    os.makedirs(output_dir, exist_ok=True)
    safe_name = metric_name.replace(" ", "_").replace(":", "").replace("/", "_")[:60]

    try:
        if analysis_type == "distribution":
            return _chart_distribution(df, required_columns[0], safe_name, output_dir, metric_name)
        elif analysis_type == "trend":
            return _chart_trend(df, required_columns, safe_name, output_dir, metric_name)
        elif analysis_type == "correlation":
            return _chart_correlation(df, required_columns, safe_name, output_dir, metric_name)
        elif analysis_type == "group_comparison":
            return _chart_group_comparison(df, required_columns, safe_name, output_dir, metric_name)
        elif analysis_type == "anomaly":
            return _chart_anomaly(df, required_columns[0], safe_name, output_dir, metric_name)
        elif analysis_type == "frequency":
            return _chart_frequency(df, required_columns[0], safe_name, output_dir, metric_name)
        elif analysis_type == "regression":
            return _chart_regression(df, required_columns, safe_name, output_dir, metric_name)
        else:
            return _chart_distribution(df, required_columns[0], safe_name, output_dir, metric_name)
    except Exception as e:
        return {"status": "error", "error": str(e), "metric": metric_name}


def save_chart(fig, filepath: str, chart_format: str = "png") -> str:
    """Save a matplotlib figure to file."""
    fig.savefig(filepath, dpi=150, bbox_inches="tight", facecolor="#1a1a2e")
    plt.close(fig)
    return filepath


def _chart_distribution(df, col, safe_name, output_dir, title):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5), facecolor="#1a1a2e")
    
    for ax in axes:
        ax.set_facecolor("#16213e")
        ax.tick_params(colors="white")
        ax.xaxis.label.set_color("white")
        ax.yaxis.label.set_color("white")
        ax.title.set_color("white")
        for spine in ax.spines.values():
            spine.set_color("#e94560")

    data = df[col].dropna()
    axes[0].hist(data, bins=30, color="#e94560", alpha=0.8, edgecolor="#0f3460")
    axes[0].set_title(f"Histogram: {col}", fontsize=12)
    axes[0].set_xlabel(col)
    axes[0].set_ylabel("Frequency")

    bp = axes[1].boxplot(data, patch_artist=True, boxprops=dict(facecolor="#e94560", color="white"),
                         medianprops=dict(color="#00ff88"), whiskerprops=dict(color="white"),
                         capprops=dict(color="white"), flierprops=dict(marker="o", markerfacecolor="#ff6b6b"))
    axes[1].set_title(f"Box Plot: {col}", fontsize=12)

    fig.suptitle(title, fontsize=14, color="white", fontweight="bold")
    plt.tight_layout()
    
    filepath = os.path.join(output_dir, f"{safe_name}.png")
    save_chart(fig, filepath)
    return {"status": "success", "file_path": filepath, "chart_type": "distribution"}


def _chart_trend(df, columns, safe_name, output_dir, title):
    dt_col, num_col = columns[0], columns[1]
    df_sorted = df.copy()
    df_sorted[dt_col] = pd.to_datetime(df_sorted[dt_col], errors="coerce")
    df_sorted = df_sorted.dropna(subset=[dt_col, num_col]).sort_values(dt_col)

    fig, ax = plt.subplots(figsize=(14, 6), facecolor="#1a1a2e")
    ax.set_facecolor("#16213e")
    ax.plot(df_sorted[dt_col], df_sorted[num_col], color="#e94560", linewidth=1.5, alpha=0.7)
    
    if len(df_sorted) > 10:
        window = max(len(df_sorted) // 20, 3)
        rolling = df_sorted[num_col].rolling(window=window).mean()
        ax.plot(df_sorted[dt_col], rolling, color="#00ff88", linewidth=2, label=f"Rolling Avg ({window})")
        ax.legend(facecolor="#16213e", edgecolor="#e94560", labelcolor="white")

    ax.set_title(title, fontsize=14, color="white", fontweight="bold")
    ax.set_xlabel(dt_col, color="white")
    ax.set_ylabel(num_col, color="white")
    ax.tick_params(colors="white")
    for spine in ax.spines.values():
        spine.set_color("#e94560")

    plt.tight_layout()
    filepath = os.path.join(output_dir, f"{safe_name}.png")
    save_chart(fig, filepath)
    return {"status": "success", "file_path": filepath, "chart_type": "trend"}


def _chart_correlation(df, columns, safe_name, output_dir, title):
    numeric_df = df[columns].select_dtypes(include=[np.number])
    if numeric_df.shape[1] < 2:
        return {"status": "error", "error": "Need at least 2 numeric columns for correlation"}

    corr = numeric_df.corr()
    fig, ax = plt.subplots(figsize=(max(10, len(columns)), max(8, len(columns) * 0.8)), facecolor="#1a1a2e")
    ax.set_facecolor("#16213e")

    im = ax.imshow(corr.values, cmap="RdYlGn", aspect="auto", vmin=-1, vmax=1)
    ax.set_xticks(range(len(corr.columns)))
    ax.set_yticks(range(len(corr.columns)))
    ax.set_xticklabels(corr.columns, rotation=45, ha="right", color="white", fontsize=9)
    ax.set_yticklabels(corr.columns, color="white", fontsize=9)

    for i in range(len(corr)):
        for j in range(len(corr)):
            ax.text(j, i, f"{corr.values[i, j]:.2f}", ha="center", va="center",
                    color="black", fontsize=8, fontweight="bold")

    cbar = plt.colorbar(im, ax=ax)
    cbar.ax.yaxis.set_tick_params(color="white")
    plt.setp(plt.getp(cbar.ax.axes, "yticklabels"), color="white")

    ax.set_title(title, fontsize=14, color="white", fontweight="bold")
    plt.tight_layout()

    filepath = os.path.join(output_dir, f"{safe_name}.png")
    save_chart(fig, filepath)
    return {"status": "success", "file_path": filepath, "chart_type": "correlation"}


def _chart_group_comparison(df, columns, safe_name, output_dir, title):
    cat_col, num_col = columns[0], columns[1]
    grouped = df.groupby(cat_col)[num_col].agg(["mean", "median", "std", "count"]).reset_index()
    grouped = grouped.sort_values("mean", ascending=True).tail(15)

    fig, ax = plt.subplots(figsize=(12, max(6, len(grouped) * 0.4)), facecolor="#1a1a2e")
    ax.set_facecolor("#16213e")

    bars = ax.barh(grouped[cat_col].astype(str), grouped["mean"], color="#e94560", alpha=0.85, edgecolor="#0f3460")
    ax.set_title(title, fontsize=14, color="white", fontweight="bold")
    ax.set_xlabel(f"Mean {num_col}", color="white")
    ax.tick_params(colors="white")
    for spine in ax.spines.values():
        spine.set_color("#e94560")

    plt.tight_layout()
    filepath = os.path.join(output_dir, f"{safe_name}.png")
    save_chart(fig, filepath)
    return {"status": "success", "file_path": filepath, "chart_type": "group_comparison"}


def _chart_anomaly(df, col, safe_name, output_dir, title):
    data = df[col].dropna()
    q1 = data.quantile(0.25)
    q3 = data.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outliers = data[(data < lower) | (data > upper)]

    fig, ax = plt.subplots(figsize=(14, 6), facecolor="#1a1a2e")
    ax.set_facecolor("#16213e")
    ax.scatter(range(len(data)), data, color="#0f3460", alpha=0.5, s=10, label="Normal")
    if len(outliers) > 0:
        outlier_idx = data[(data < lower) | (data > upper)].index
        ax.scatter(outlier_idx, outliers, color="#e94560", s=30, zorder=5, label=f"Outliers ({len(outliers)})")
    ax.axhline(y=upper, color="#00ff88", linestyle="--", alpha=0.6, label=f"Upper: {upper:.2f}")
    ax.axhline(y=lower, color="#ff6b6b", linestyle="--", alpha=0.6, label=f"Lower: {lower:.2f}")
    ax.legend(facecolor="#16213e", edgecolor="#e94560", labelcolor="white")
    ax.set_title(title, fontsize=14, color="white", fontweight="bold")
    ax.tick_params(colors="white")
    for spine in ax.spines.values():
        spine.set_color("#e94560")

    plt.tight_layout()
    filepath = os.path.join(output_dir, f"{safe_name}.png")
    save_chart(fig, filepath)
    return {"status": "success", "file_path": filepath, "chart_type": "anomaly"}


def _chart_frequency(df, col, safe_name, output_dir, title):
    counts = df[col].value_counts().head(15)

    fig, ax = plt.subplots(figsize=(12, 6), facecolor="#1a1a2e")
    ax.set_facecolor("#16213e")
    bars = ax.bar(range(len(counts)), counts.values, color="#e94560", alpha=0.85, edgecolor="#0f3460")
    ax.set_xticks(range(len(counts)))
    ax.set_xticklabels([str(x)[:20] for x in counts.index], rotation=45, ha="right", color="white", fontsize=9)
    ax.set_title(title, fontsize=14, color="white", fontweight="bold")
    ax.set_ylabel("Count", color="white")
    ax.tick_params(colors="white")
    for spine in ax.spines.values():
        spine.set_color("#e94560")

    plt.tight_layout()
    filepath = os.path.join(output_dir, f"{safe_name}.png")
    save_chart(fig, filepath)
    return {"status": "success", "file_path": filepath, "chart_type": "frequency"}


def _chart_regression(df, columns, safe_name, output_dir, title):
    c1, c2 = columns[0], columns[1]
    clean = df[[c1, c2]].dropna()

    fig, ax = plt.subplots(figsize=(10, 8), facecolor="#1a1a2e")
    ax.set_facecolor("#16213e")
    ax.scatter(clean[c1], clean[c2], color="#e94560", alpha=0.5, s=15)

    try:
        z = np.polyfit(clean[c1], clean[c2], 1)
        p = np.poly1d(z)
        x_line = np.linspace(clean[c1].min(), clean[c1].max(), 100)
        ax.plot(x_line, p(x_line), color="#00ff88", linewidth=2, label=f"y = {z[0]:.3f}x + {z[1]:.3f}")
        ax.legend(facecolor="#16213e", edgecolor="#e94560", labelcolor="white")
    except:
        pass

    ax.set_title(title, fontsize=14, color="white", fontweight="bold")
    ax.set_xlabel(c1, color="white")
    ax.set_ylabel(c2, color="white")
    ax.tick_params(colors="white")
    for spine in ax.spines.values():
        spine.set_color("#e94560")

    plt.tight_layout()
    filepath = os.path.join(output_dir, f"{safe_name}.png")
    save_chart(fig, filepath)
    return {"status": "success", "file_path": filepath, "chart_type": "regression"}
