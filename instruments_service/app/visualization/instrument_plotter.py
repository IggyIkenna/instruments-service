"""
Instrument visualization utilities

Visualize instrument definitions, availability, and metadata for analysis and debugging.
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from typing import List, Dict, Optional, Union
import logging

logger = logging.getLogger(__name__)


class InstrumentPlotter:
    """
    Plotter for visualizing instrument data.

    Supports:
    - Instrument availability over time
    - Venue distribution
    - Instrument type distribution
    - Data type availability
    - Interactive plotly charts
    """

    def __init__(self):
        """Initialize the instrument plotter."""
        self.figure = None

    def plot_instrument_availability(
        self,
        instruments: pd.DataFrame,
        title: str = "Instrument Availability Timeline",
        height: int = 600,
    ) -> go.Figure:
        """
        Plot instrument availability over time.

        Args:
            instruments: DataFrame with instrument definitions
            title: Chart title
            height: Chart height in pixels

        Returns:
            Plotly figure
        """
        fig = go.Figure()

        # Group by date and count instruments
        if "available_from_datetime" in instruments.columns:
            instruments["date"] = pd.to_datetime(
                instruments["available_from_datetime"]
            ).dt.date
            availability = instruments.groupby("date").size().reset_index(name="count")

            fig.add_trace(
                go.Scatter(
                    x=availability["date"],
                    y=availability["count"],
                    mode="lines+markers",
                    name="Available Instruments",
                    line=dict(color="blue", width=2),
                    marker=dict(size=6),
                )
            )

        fig.update_layout(
            title=title,
            height=height,
            xaxis_title="Date",
            yaxis_title="Number of Instruments",
            hovermode="x unified",
        )

        return fig

    def plot_venue_distribution(
        self,
        instruments: pd.DataFrame,
        title: str = "Instrument Distribution by Venue",
        height: int = 600,
    ) -> go.Figure:
        """
        Plot distribution of instruments by venue.

        Args:
            instruments: DataFrame with instrument definitions
            title: Chart title
            height: Chart height in pixels

        Returns:
            Plotly figure
        """
        if "venue" not in instruments.columns:
            raise ValueError("DataFrame must contain 'venue' column")

        venue_counts = instruments["venue"].value_counts()

        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                x=venue_counts.index,
                y=venue_counts.values,
                marker=dict(color="lightblue"),
            )
        )

        fig.update_layout(
            title=title,
            height=height,
            xaxis_title="Venue",
            yaxis_title="Number of Instruments",
            showlegend=False,
        )

        return fig

    def plot_instrument_type_distribution(
        self,
        instruments: pd.DataFrame,
        title: str = "Instrument Distribution by Type",
        height: int = 600,
    ) -> go.Figure:
        """
        Plot distribution of instruments by type.

        Args:
            instruments: DataFrame with instrument definitions
            title: Chart title
            height: Chart height in pixels

        Returns:
            Plotly figure
        """
        if "instrument_type" not in instruments.columns:
            raise ValueError("DataFrame must contain 'instrument_type' column")

        type_counts = instruments["instrument_type"].value_counts()

        fig = go.Figure()

        fig.add_trace(
            go.Pie(labels=type_counts.index, values=type_counts.values, hole=0.4)
        )

        fig.update_layout(title=title, height=height, showlegend=True)

        return fig

    def plot_data_type_availability(
        self,
        instruments: pd.DataFrame,
        title: str = "Data Type Availability",
        height: int = 600,
    ) -> go.Figure:
        """
        Plot availability of different data types across instruments.

        Args:
            instruments: DataFrame with instrument definitions
            title: Chart title
            height: Chart height in pixels

        Returns:
            Plotly figure
        """
        if "data_types" not in instruments.columns:
            raise ValueError("DataFrame must contain 'data_types' column")

        # Count data types
        data_type_counts = {}
        for types_str in instruments["data_types"].dropna():
            if isinstance(types_str, str):
                for dt in types_str.split(","):
                    dt = dt.strip()
                    data_type_counts[dt] = data_type_counts.get(dt, 0) + 1

        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                x=list(data_type_counts.keys()),
                y=list(data_type_counts.values()),
                marker=dict(color="lightgreen"),
            )
        )

        fig.update_layout(
            title=title,
            height=height,
            xaxis_title="Data Type",
            yaxis_title="Number of Instruments",
            showlegend=False,
        )

        return fig

