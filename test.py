import pandas as pd
import numpy as np
from dash import Dash, dcc, html
import plotly.express as px
import plotly.graph_objects as go



# =========================================================
# 🔹 1. Efficiency overview (bar chart)
# =========================================================
import plotly.graph_objects as go
import pandas as pd

def create_efficiency_bar_with_runtime(metrics):
    # Melt efficiencies for bar plotting
    df_melt = metrics.melt(
        id_vars=["process_label", "process_display"],
        value_vars=[
            "avg_cpu_efficiency",
            "avg_mem_efficiency",
            "avg_time_efficiency",
        ],
        var_name="metric",
        value_name="efficiency",
    )

    # --- Bar trace for efficiencies ---
    fig = go.Figure()

    for metric_name, df_metric in df_melt.groupby("metric"):
        fig.add_trace(
            go.Bar(
                x=df_metric["process_label"],
                y=df_metric["efficiency"],
                name=metric_name.replace("_", " ").title(),
                hovertext=df_metric["process_display"],
                hovertemplate=(
                    "<b>%{hovertext}</b><br>"
                    "Efficiency: %{y:.1f}%<extra></extra>"
                ),
            )
        )

    # --- Add runtime as a line on secondary y-axis ---
    fig.add_trace(
        go.Scatter(
            x=metrics["process_label"],
            y=metrics["total_runtime_hours"],  # or avg_runtime_hours
            name="Total Runtime (h)",
            yaxis="y2",
            mode="lines+markers",
            line=dict(color="black", width=2, dash="dot"),
            marker=dict(size=6),
            hovertext=metrics["process_display"],
            hovertemplate=(
                "<b>%{hovertext}</b><br>"
                "Total Runtime: %{y:.1f} h<extra></extra>"
            ),
        )
    )

    # --- Layout with secondary y-axis ---
    fig.update_layout(
        title="Process Efficiency & Runtime Overview",
        xaxis_title="Process",
        yaxis=dict(title="Efficiency (%)"),
        yaxis2=dict(
            title="Total Runtime (h)",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
        barmode="group",
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=450,
    )

    # Optional: replace x-tick labels with display labels
    label_map = dict(zip(metrics["process_label"], metrics["process_display"]))
    fig.update_xaxes(
        tickmode="array",
        tickvals=list(label_map.keys()),
        ticktext=list(label_map.values()),
    )

    return fig

# =========================================================
# 🔹 2. Optimization priority (your improved bubble plot)
# =========================================================
def create_bubble(metrics):
    sizes = np.sqrt(metrics["computational_burden"])
    sizes = 10 + 60 * (sizes / sizes.max())

    fig = px.scatter(
        metrics,
        x="total_cpu_hours_wasted",
        y="total_mem_gb_hours_wasted",
        size=sizes,
        color="overall_efficiency",
        hover_name="process_display",
        color_continuous_scale="RdYlGn",
    )

    fig.update_layout(
        title="Optimization Priority Map",
        xaxis_title="CPU-hours wasted",
        yaxis_title="Memory GB-hours wasted",
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=500,
        coloraxis_colorbar=dict(title="Efficiency"),
    )

    fig.add_annotation(
        text="Top-right = highest optimization priority",
        xref="paper",
        yref="paper",
        x=0.99,
        y=0.98,
        showarrow=False,
    )

    return fig


# =========================================================
# 🔹 3. Efficiency score vs impact
# =========================================================
def create_score_plot(metrics):
    fig = px.scatter(
        metrics,
        x="overall_efficiency",
        y="computational_burden",
        size="n_tasks",
        hover_name="process_display",
    )

    fig.update_layout(
        title="Efficiency vs Computational Impact",
        xaxis_title="Efficiency Score",
        yaxis_title="Computational Burden",
        plot_bgcolor="white",
        paper_bgcolor="white",
        height=400,
    )

    return fig


