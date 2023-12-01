"""Teradata target class."""

from __future__ import annotations

from singer_sdk import typing as th
from singer_sdk.target_base import SQLTarget

from target_teradata.sinks import (
    TeradataSink,
)


class TargetTeradata(SQLTarget):
    """Sample target for Teradata."""

    name = "target-teradata"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="SQLAlchemy connection string",
        ),
    ).to_dict()

    default_sink_class = TeradataSink


if __name__ == "__main__":
    TargetTeradata.cli()
