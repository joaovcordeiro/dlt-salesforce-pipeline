import os
import fsspec
import posixpath
from fsspec import AbstractFileSystem
from fsspec.core import url_to_fs

import dlt

fs: AbstractFileSystem

# take from the config shared with the destination
# get bucket_url from config/secrets
BUCKET_URL = dlt.config["sources.filesystem.bucket_url"]
# get aws credentials from config/secrets
destination_config = dlt.secrets["destination.credentials"]
# pass to fsspec
fs, _ = url_to_fs(BUCKET_URL, token=destination_config)

# uncomment if the credentials are available as default
# BUCKET_URL="s3://dlt-ci-test-bucket/state"
# fs, _ = url_to_fs(BUCKET_URL)


def restore(pipeline: dlt.Pipeline) -> dlt.Pipeline:
    state_path = posixpath.join(BUCKET_URL, "state", pipeline.pipeline_name) + "/"
    working_dir = os.path.join(pipeline.working_dir)

    # state folder exists
    if fs.isdir(state_path):
        fs.get(state_path, working_dir, recursive=True)
        return dlt.attach(pipeline.pipeline_name)
    # or does not - return existing instance
    return pipeline


def backup(pipeline: dlt.Pipeline) -> dlt.Pipeline:
    state_path = posixpath.join(BUCKET_URL, "state", pipeline.pipeline_name) + "/"
    working_dir = os.path.join(pipeline.working_dir)
    # copy state
    fs.put(
        os.path.join(working_dir, dlt.Pipeline.STATE_FILE),
        posixpath.join(state_path, dlt.Pipeline.STATE_FILE),
    )
    # copy schemas
    _, schemas_folder = os.path.split(pipeline._schema_storage.storage.storage_path)

    fs.put(
        pipeline._schema_storage.storage.storage_path,
        posixpath.join(state_path, schemas_folder),
        recursive=True,
    )


# @dlt.source
# def source1():

#     @dlt.resource
#     def numbers():
#         dlt.current.resource_state().setdefault("runs", 0)
#         yield [1, 2, 3]
#         dlt.current.resource_state()["runs"] += 1

#     return numbers


# @dlt.source
# def source2():

#     @dlt.resource
#     def letters():
#         dlt.current.resource_state().setdefault("runs", 0)
#         yield ["A", "B", "C"]
#         dlt.current.resource_state()["runs"] += 1

#     return letters


# pipeline = dlt.pipeline(
#     pipeline_name="filesystem_test", dataset_name="dataset", destination="filesystem"
# )
# pipeline = (
#     pipeline.drop()
# )  # make sure the state is wiped out, not needed on github actions
# pipeline = restore(pipeline)
# info = pipeline.run([source1(), source2()])
# print(info)
# backup(pipeline)
