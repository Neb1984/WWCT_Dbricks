"""OANDA Exchange Rates source connector."""

from databricks.labs.community_connector.sources.oanda.oanda import (
    OandaLakeflowConnect,
)


from databricks.labs.community_connector.sparkpds import LakeflowSource


class OandaDataSource(LakeflowSource):
    _lakeflow_connect_cls = OandaLakeflowConnect
    # Override the Spark format name with the source name once this no
    # longer relies on UC connection-option injection. Kept as the default
    # "lakeflow_connect" for now so existing pipelines keep working.
    # _format_name = "oanda"


__all__ = ["OandaLakeflowConnect",
    "OandaDataSource",
]
