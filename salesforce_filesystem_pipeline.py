import dlt
from dlt.sources import DltResource
from dlt.sources import incremental
from dlt.common.typing import TDataItem

from typing import Iterable

from simple_salesforce import Salesforce

from salesforce.helpers import get_records

from utils.state_restore import restore, backup

import logging

logger = logging.getLogger(__name__)


def create_resource(client, sobject) -> DltResource:
    """
    Creates a resource for a Salesforce object.

    Args:
        client (Salesforce): The Salesforce client.
        sobject (str): The Salesforce object name.

    Returns:
        DltResource: The resource for the Salesforce object.
    """

    @dlt.resource(name=sobject.lower(), primary_key="Id", write_disposition="append")
    def get_incremental_data(
        # Define the incremental state
        last_timestamp: incremental[str] = dlt.sources.incremental(
            "SystemModstamp",
            initial_value="1970-01-01T00:00:00Z",
        )
    ) -> Iterable[TDataItem]:
        """
        Retrieves incremental data from Salesforce.

        Args:
            last_timestamp (incremental[str]): The last timestamp value.

        Yields:
            TDataItem: The data items from Salesforce.
        """
        yield get_records(client, sobject, last_timestamp.last_value, "SystemModstamp")

    return get_incremental_data


@dlt.source(name="salesforce")
def salesforce_source(
    user_name: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    security_token: str = dlt.secrets.value,
    sobjects: list[str] = dlt.config.value,
) -> Iterable[DltResource]:
    """
    Retrieves data from Salesforce using the Salesforce API.

    Args:
        user_name (str): The username for authentication. Defaults to the value in the `dlt.secrets` object.
        password (str): The password for authentication. Defaults to the value in the `dlt.secrets` object.
        security_token (str): The security token for authentication. Defaults to the value in the `dlt.secrets` object.

    Yields:
        DltResource: Data resources from Salesforce.
    """

    # Connect to Salesforce
    try:
        client = Salesforce(user_name, password, security_token)
    except Exception as e:
        logger.error(f"Failed to connect to Salesforce: {e}")
        raise

    # Create a resource for each sobject
    for sobject in sobjects:
        yield create_resource(client, sobject)


if __name__ == "__main__":
    """
    The main function for the pipeline.

    This function creates a pipeline that retrieves data from Salesforce and stores it in the filesystem.

    Args:
        pipeline_name (str): The name of the pipeline.
        destination (str): The destination of the pipeline.
        dataset_name (str): The name of the dataset, will be used as the folder name in the filesystem.
        progress (str): The progress of the pipeline.
    """
    pipeline = dlt.pipeline(
        pipeline_name="salesforce_filesystem",
        destination="filesystem",
        dataset_name="salesforce",
        progress="log",
    )

    # Remove the existing pipeline state
    pipeline = pipeline.drop()

    # Restore the pipeline state
    pipeline = restore(pipeline)

    # Run the pipeline
    load_info = pipeline.run(salesforce_source())

    print(load_info)

    # Backup the pipeline state, so that it can be restored later
    backup(pipeline)
