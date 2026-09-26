"""Cloudera WebHDFS community connector."""

from databricks.labs.community_connector.sources.cloudera_webhdfs.cloudera_webhdfs import (
    ClouderaWebhdfsLakeflowConnect,
)
from databricks.labs.community_connector.sparkpds import LakeflowSource


class ClouderaWebhdfsDataSource(LakeflowSource):
    """Spark data source entry point for the connector."""

    _lakeflow_connect_cls = ClouderaWebhdfsLakeflowConnect


__all__ = ["ClouderaWebhdfsLakeflowConnect", "ClouderaWebhdfsDataSource"]
