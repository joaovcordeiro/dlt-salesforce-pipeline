from typing import Iterator

from dlt.sources import DltResource
from typing import Iterable

import dlt
from dlt.sources import TDataItems


try:
    from .filesystem import filesystem, read_jsonl  # type: ignore
except ImportError:
    from filesystem import (
        filesystem,
        read_jsonl,
    )


@dlt.source(name="filesystem")
def filesystem_source(
    bucket_url=dlt.config["sources.filesystem.bucket_url"],
) -> Iterable[DltResource]:

    @dlt.resource(name="cancelamento", primary_key="id", write_disposition="merge")
    def cancelamento() -> Iterator[TDataItems]:
        files = filesystem(bucket_url, file_glob="salesforce/cancelamento_c/*.jsonl")

        files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
        json = (files | read_jsonl()).with_name("cancelamento")

        for item in json:
            yield item

    @dlt.resource(name="opportunity", primary_key="id", write_disposition="merge")
    def opportunity() -> Iterator[TDataItems]:
        files = filesystem(bucket_url, file_glob="salesforce/opportunity/*.jsonl")

        files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
        json = (files | read_jsonl()).with_name("opportunity")

        for item in json:
            yield item

    @dlt.resource(name="lead", primary_key="id", write_disposition="merge")
    def lead() -> Iterator[TDataItems]:
        files = filesystem(bucket_url, file_glob="salesforce/lead/*.jsonl")

        files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
        json = (files | read_jsonl()).with_name("lead")

        for item in json:
            yield item

    @dlt.resource(name="contact", primary_key="id", write_disposition="merge")
    def contact() -> Iterator[TDataItems]:
        files = filesystem(bucket_url, file_glob="salesforce/contact/*.jsonl")

        files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
        json = (files | read_jsonl()).with_name("contact")

        for item in json:
            yield item

    @dlt.resource(name="account", primary_key="id", write_disposition="merge")
    def account() -> Iterator[TDataItems]:
        files = filesystem(bucket_url, file_glob="salesforce/account/*.jsonl")

        files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
        json = files | read_jsonl().with_name("account")

        for item in json:
            yield item

    return (opportunity, lead, account, contact, cancelamento)


if __name__ == "__main__":
    pipeline = dlt.pipeline(
        pipeline_name="filesystem_duckdb",
        destination="duckdb",
        dataset_name="salesforce",
        progress="log",
    )
    pipeline.run(filesystem_source())
