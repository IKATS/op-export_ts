"""
Copyright 2018 CS Systèmes d'Information

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import logging
from functools import partial
import os
import time
import uuid
from multiprocessing import Pool
from ikats.core.resource.api import IkatsApi

LOGGER = logging.getLogger(__name__)
DESTINATION_PATH = os.environ.get('TSDATA', '/tmp/export_data')

# Fallback pattern used when error occurred during placeholders replacement in pattern
FALLBACK_PATTERN = '/{fid}.csv'


def export_ts(ds_name, pattern):
    """
    Tool to export all timeseries in a dataset to CSV files (one file per TS)
    User provides a pattern that use python string format to decide the relative path for each timeseries
    Destination path is prepended to the relative path to create an absolute path

    :param ds_name: Name of the dataset to export
    :param pattern: pattern used to build the destination tree

    :type ds_name: str
    :type pattern: str

    :return: status
    :rtype: dict
    """

    start_time = time.time()

    # Output path is unique thanks to uuid
    out_path = "%s/%s" % (DESTINATION_PATH, str(uuid.uuid4()))

    # Checks for permission to write to folder
    # Only needed for folders that exist already
    if os.path.isdir(DESTINATION_PATH):
        if not os.access(DESTINATION_PATH, os.W_OK):
            LOGGER.warning("Permission denied:" + DESTINATION_PATH)
            raise PermissionError("Permission denied:" + DESTINATION_PATH)

    ts_list = IkatsApi.ds.read(ds_name)['ts_list']

    # Note: Datasets that do not exist return empty lists
    if not ts_list:
        LOGGER.info("Empty dataset %s or does not exist", ds_name)
        return

    LOGGER.debug("%s timeseries found in %s", len(ts_list), ds_name)

    partial_export = partial(export_time_series, ds_name=ds_name, destination_path=out_path, pattern=pattern)

    pool = Pool(processes=min(len(ts_list), os.cpu_count()))

    pool.map(partial_export, ts_list)

    total_points_in_all_ts = sum(int(metadata['qual_nb_points']) for metadata in IkatsApi.md.read(ts_list).values())

    # Elapsed time in milliseconds
    time_elapsed = time.time() - start_time

    return {
        "path": out_path,
        "ts_count": len(ts_list),
        "points_count": total_points_in_all_ts,
        "duration": time_elapsed
    }


def get_metadata(tsuid, pattern):
    """
    Get metadata from md api
    If fid is in the user provided pattern add that to metadata

    :param tsuid: TSUID to get metadata from IKATS
    :param pattern: Pattern containing the metadata keys we will need

    :type tsuid: str
    :type pattern: str

    :return: the metadata list
    :rtype dict
    """
    metadata = IkatsApi.md.read(tsuid)[tsuid]

    if "{fid}" in pattern:
        metadata['fid'] = IkatsApi.ts.fid(tsuid)

    return metadata


def create_directory(pattern, destination_path):
    """
    Creates, if needed, the directory where the resulting CSV will be generated

    :param pattern: Path pattern to use for generating the files
    :param destination_path: Root directory from which to create the directory tree

    :type pattern: str
    :type destination_path: str

    :return: The created path
    :rtype: str
    """

    path = "/".join([destination_path, pattern])
    path = path.replace("//", "/")

    if os.path.exists(path):
        LOGGER.warning("File already exists" + path)
        raise ValueError("File already exists" + path)

    directory = os.path.split(path)[0]

    try:
        os.makedirs(directory, exist_ok=True)
        LOGGER.debug("successfully created path %s", directory)

    except OSError as ose:
        LOGGER.warning("Permission denied creating directory: %s", directory)
        raise ose

    LOGGER.debug("Directory for %s created", path)
    return path


def fetch_and_write_time_series(path, tsuid):
    """
    Read the timeseries from IKATS API
    Convert first column to numpy datetime 64
    Write each timestamp, value to the file followed by a newline

    :param path: path to the CSV file that will get the data
    :param tsuid: TSUID to get points from

    :type path: str
    :type tsuid: str
    """

    time_series = IkatsApi.ts.read(tsuid)[0]
    if len(time_series):
        zipped_ts = zip(time_series[:, 0].astype('datetime64[ms]'), time_series[:, 1])

        with open(path, 'w') as filename:
            filename.write("Date;Value\n")

            for timestamp, value in zipped_ts:
                filename.write(";".join([str(timestamp), str(value)]) + "\n")

        LOGGER.debug("Timeseries %s exported", tsuid)
    else:
        LOGGER.warning("Timeseries %s is empty", tsuid)


def export_time_series(tsuid, ds_name, destination_path, pattern):
    """

    :param tsuid: the tsuid that identifies the series
    :param ds_name: Dataset name to be used as placeholder 'ds' in output path
    :param destination_path: absolute folder to write CSV's
    :param pattern: use python format to create relative path (from destination_path)
    to write CSV's

    :type tsuid: str
    :type ds_name: str
    :type destination_path: str
    :type pattern: str
    """
    LOGGER.debug("Starting ETS for %s process by acquiring Metadata", tsuid)
    metadata = get_metadata(tsuid=tsuid, pattern=pattern)
    metadata["ds"] = ds_name

    try:
        filled_pattern = pattern.format(**metadata)
    except KeyError as ex:
        LOGGER.warning("Key not found in pattern for %s, using Fallback pattern. %s", metadata["fid"], ex)
        filled_pattern = FALLBACK_PATTERN.format(**metadata)

    path = create_directory(destination_path=destination_path, pattern=filled_pattern)
    fetch_and_write_time_series(path=path, tsuid=tsuid)


# status = export_ts(ds_name="Portfolio", pattern="{ds}/{fid}.csv")
# print(status)
# print(__name__)
