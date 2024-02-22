import os
import posixpath
from typing import Iterator

import re

import dlt
from dlt.sources import TDataItems

from filesystem.helpers import FilesystemConfigurationResource

try:
    from .filesystem import FileItemDict, filesystem, readers, read_csv, read_jsonl  # type: ignore
except ImportError:
    from filesystem import (
        FileItemDict,
        filesystem,
        readers,
        read_csv,
        read_jsonl,
    )


BUCKET_URL = dlt.config["sources.filesystem.bucket_url"]


# @dlt.source
# def read_bucket_files(
#     bucket_url: str = dlt.config["sources.filesystem.bucket_url"],
#     sobjects: list[str] = dlt.config["sources.sobjects"],
# ) -> None:
#     # for sobject in sobjects:
#     #     sobject = re.sub(r"_{2,}", "_", sobject)

#     #     # JSONL reading
#     #     jsonl_reader = readers(bucket_url, file_glob=f"{sobject}/*.jsonl").read_jsonl(
#     #         chunksize=10000
#     #     )
#     #     yield jsonl_reader.with_name({sobject})

#     sobject = "cancelamento_c"

#     files = filesystem(bucket_url, file_glob=f"{sobject}/*.jsonl")
#     for file in files:
#         print(file.file_name)


def read_parquet_and_jsonl_chunked(dataset, sobject) -> None:
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem",
        destination="duckdb",
        dataset_name="teams_data",
    )
    # When using the readers resource, you can specify a filter to select only the files you
    # want to load including a glob pattern. If you use a recursive glob pattern, the filenames
    # will include the path to the file inside the bucket_url.

    sobject = re.sub(r"_{2,}", "_", sobject)
    # JSONL reading (in large chunks!)
    jsonl_reader = readers(
        BUCKET_URL, file_glob=f"{dataset}/{sobject}/*.jsonl"
    ).read_jsonl(chunksize=10000)

    load_info = pipeline.run(
        jsonl_reader.with_name({sobject}),
    )

    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


def read_custom_file_type_excel() -> None:
    """Here we create an extract pipeline using filesystem resource and read_csv transformer"""

    # instantiate filesystem directly to get list of files (FileItems) and then use read_excel transformer to get
    # content of excel via pandas

    @dlt.transformer(standalone=True)
    def read_excel(
        items: Iterator[FileItemDict], sheet_name: str
    ) -> Iterator[TDataItems]:
        import pandas as pd

        for file_obj in items:
            with file_obj.open() as file:
                yield pd.read_excel(file, sheet_name).to_dict(orient="records")

    freshman_xls = filesystem(
        bucket_url=TESTS_BUCKET_URL, file_glob="../custom/freshman_kgs.xlsx"
    ) | read_excel("freshman_table")

    load_info = dlt.run(
        freshman_xls.with_name("freshman"),
        destination="bigquery",
        dataset_name="freshman_data",
    )
    print(load_info)


def copy_files_resource(local_folder: str) -> None:
    """Demonstrates how to copy files locally by adding a step to filesystem resource and the to load the download listing to db"""
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem_copy",
        destination="bigquery",
        dataset_name="standard_filesystem_data",
    )

    # a step that copies files into test storage
    def _copy(item: FileItemDict) -> FileItemDict:
        # instantiate fsspec and copy file
        dest_file = os.path.join(local_folder, item["file_name"])
        # create dest folder
        os.makedirs(os.path.dirname(dest_file), exist_ok=True)
        # download file
        item.fsspec.download(item["file_url"], dest_file)
        # return file item unchanged
        return item

    # use recursive glob pattern and add file copy step
    downloader = filesystem(TESTS_BUCKET_URL, file_glob="**").add_map(_copy)

    # NOTE: you do not need to load any data to execute extract, below we obtain
    # a list of files in a bucket and also copy them locally
    # listing = list(downloader)
    # print(listing)

    # download to table "listing"
    # downloader = filesystem(TESTS_BUCKET_URL, file_glob="**").add_map(_copy)
    load_info = pipeline.run(
        downloader.with_name("listing"), write_disposition="replace"
    )
    # pretty print the information on data that was loaded
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


def read_files_incrementally_mtime() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="standard_filesystem_incremental",
        destination="duckdb",
        dataset_name="file_tracker",
    )

    # here we modify filesystem resource so it will track only new csv files
    # such resource may be then combined with transformer doing further processing
    new_files = filesystem(
        bucket_url=BUCKET_URL, file_glob="salesforce/cancelamento_c/*.jsonl"
    )
    # add incremental on modification time
    new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    load_info = pipeline.run((new_files | read_jsonl()).with_name("json_files"))
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)

    # load again - no new files!
    new_files = filesystem(
        bucket_url=BUCKET_URL, file_glob="salesforce/cancelamento_c/*.jsonl"
    )
    # add incremental on modification time
    new_files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    load_info = pipeline.run((new_files | read_jsonl()).with_name("json_files"))
    print(load_info)
    print(pipeline.last_trace.last_normalize_info)


def teste():
    pipeline = dlt.pipeline(
        pipeline_name="teste",
        destination="duckdb",
        dataset_name="files",
    )

    @dlt.resource(name="cancelamento_c")

    sobject = "cancelamento_c"

    files = filesystem(BUCKET_URL, file_glob=f"salesforce/{sobject}/*.jsonl")
    files.apply_hints(incremental=dlt.sources.incremental("modification_date"))
    for file in files:
        print(file.file_name)
    # load to met_csv table using with_name()
    pipeline.run(files.with_name("json_data"))


if __name__ == "__main__":
    teste()
