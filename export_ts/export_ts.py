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
from multiprocessing import Pool
from ikats.core.resource.api import IkatsApi

LOGGER = logging.getLogger(__name__)


def export_ts(ds_name, pattern, destination_path=None, multi_process=True, ok_to_overwrite=False):
    """
    Tool to export all timeseries in a dataset to CSV files (one file per TS)
    User provides a pattern that use python string format to decide the relative path for each timeseries
    Destination path is prepended to the relative path to create an absolute path

    :param ds_name: Name of the dataset to export
    :param pattern: pattern used to build the destination tree
    :param destination_path: location of the result
    :param multi_process: Flag activating the multi-processing (True, default) or single processing (False)
    :param ok_to_overwrite: Flag allowing to overwrite if results already exist (True), or not (False, default)

    :type ds_name: str
    :type pattern: str
    :type destination_path: str
    :type multi_process: bool
    :param ok_to_overwrite: bool

    :return: status
    :rtype: dict
    """

    start_time = time.time()

    if not destination_path:
        destination_path = "/tmp/" + ds_name.lower()
        LOGGER.debug("Setting new destination path " + destination_path)

    # If user already has files in folder and does not want to overwrite
    # We need to keep track.
    preexisting_files = set()

    # Checks for permission to write to folder
    # Only needed for folders that exist already
    if os.path.isdir(destination_path):
        if not os.access(destination_path, os.W_OK):
            LOGGER.warning("Permission denied:" + destination_path)
            raise PermissionError("Permission denied:" + destination_path)
        if os.listdir(destination_path) is not None:
            if not ok_to_overwrite:
                LOGGER.warning(
                    "Writing to non-empty directory " + destination_path)
                LOGGER.warning(" ".join(
                    [destination_path, " contains " + ", ".join(os.listdir(destination_path))]))
                raise ValueError(
                    "Attempting to write to a non-empty directory " + destination_path)
            else:
                for root, _, files in os.walk(destination_path):
                    preexisting_files.update(
                        os.path.join(root, f) for f in files)

    else:
        if not destination_path.startswith("/"):
            LOGGER.warning(
                "Destination Path must start with /. Given: %s:", destination_path)
            raise ValueError(
                "Destination Path must start with /. Given: %s:", destination_path)

    ts_list = IkatsApi.ds.read(ds_name)['ts_list']

    # Note: Datasets that do not exist return empty lists
    if not ts_list:
        LOGGER.info("Empty dataset %s or does not exist", ds_name)
        return

    LOGGER.debug("%s datasets found", len(ts_list))

    if multi_process:

        partial_export = partial(export_time_series, destination_path=destination_path,
                                 pattern=pattern, preexisting_files=preexisting_files,
                                 ok_to_overwrite=ok_to_overwrite)

        pool = Pool(processes=min(len(ts_list), os.cpu_count()))

        pool.map(partial_export, ts_list)
    else:
        for tsuid in ts_list:
            export_time_series(destination_path=destination_path, pattern=pattern,
                               tsuid=tsuid, preexisting_files=preexisting_files,
                               ok_to_overwrite=ok_to_overwrite)

    ts_processed = len(ts_list)
    total_points_in_all_ts = sum(int(
        metadata['qual_nb_points']) for metadata in IkatsApi.md.read(ts_list).values())
    time_elapsed = time.time() - start_time

    return {
        "ts_count": ts_processed,
        "points_count": total_points_in_all_ts,
        "duration": time_elapsed
    }


def get_metadata(tsuid, pattern):
    """
    Get metadata from md api
    If fid is in the user provided pattern add that to metadata

    :param tsuid: TSUID to get metadata from Ikats
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


def create_directory(pattern, destination_path, preexisting_files=set(), ok_to_overwrite=False):
    """
    Creates, if needed, the directory where the resulting CSV will be generated

    :param pattern: Path pattern to use for generating the files
    :param destination_path: Root directory from which to create the directory tree
    :param preexisting_files: List of existing files to keep
    :param ok_to_overwrite: Set to True to overwrite data if path exists (False is default)

    :type pattern: str
    :type destination_path: str
    :type preexisting_files: set
    :type ok_to_overwrite: bool

    :return: The created path
    :rtype: str
    """

    path = "/".join([destination_path, pattern])
    path = path.replace("//", "/")

    LOGGER.debug(path + " exists? " + str(os.path.exists(path)))

    if os.path.exists(path):
        if path not in preexisting_files or not ok_to_overwrite:
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
    Read the timeseries from Ikats API
    Convert first column to numpy datetime 64
    Write each timestamp, value to the file followed by a newline

    :param path: path to the CSV file that will get the data
    :param tsuid: TSUID to get points from

    :type path: str
    :type tsuid: str
    """

    time_series = IkatsApi.ts.read(tsuid)[0]
    zipped_ts = zip(time_series[:, 0].astype(
        'datetime64[ms]'), time_series[:, 1])

    with open(path, 'w') as filename:
        filename.write("Date;Value\n")

        for timestamp, value in zipped_ts:
            filename.write(";".join([str(timestamp), str(value)]))
            filename.write("\n")

    LOGGER.debug("Timeseries %s exported", tsuid)


def export_time_series(tsuid, destination_path, pattern, preexisting_files, ok_to_overwrite):
    """

    :param tsuid: the tsuid that identifies the series
    :param destination_path: absolute folder to write CSV's
    :param pattern: use python format to create relative path (from destination_path)
    to write CSV's
    :param preexisting_files: set of files path that the were already in the user's folder
    :param ok_to_overwrite: can previous timeseries be overwritten

    :type tsuid: str
    :type destination_path: str
    :type pattern: str
    :type preexisting_files: set
    :type ok_to_overwrite: bool
    """
    LOGGER.debug("Starting ETS for %s process by acquiring Metadata", tsuid)
    metadata = get_metadata(tsuid=tsuid, pattern=pattern)

    path = create_directory(destination_path=destination_path,
                            pattern=pattern.format(**metadata),
                            preexisting_files=preexisting_files,
                            ok_to_overwrite=ok_to_overwrite)
    fetch_and_write_time_series(path=path, tsuid=tsuid)
