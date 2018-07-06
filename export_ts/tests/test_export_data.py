"""
Tests of Export Data
"""
import os
import shutil
import unittest
from unittest import TestCase

from itertools import product
from collections import namedtuple

import logging

from export_ts.export_ts import export_ts, get_metadata, create_directory, fetch_and_write_time_series, LOGGER
from ikats.core.resource.api import IkatsApi

FileMetric = namedtuple('FileMetric', 'tsuid dircount filecount maxfiles files')


def log_to_stdout(logger_to_use):
    """
    Allow to print some loggers to stdout
    :param logger_to_use: the LOGGER object to redirect to stdout
    """

    logger_to_use.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(funcName)s:%(message)s')
    # Create another handler that will redirect log entries to STDOUT
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger_to_use.addHandler(stream_handler)


def count_dirs_and_files(destination_path):
    """
    Count the number of directories and files in the result path to be used for test validation

    :param destination_path: the path to get information from
    :type destination_path: str

    :return: information about the path structure
    :rtype: FileMetric
    """
    dir_count, file_count, max_files = 0, 0, 0
    all_files = []

    for root, directories, files in os.walk(destination_path):
        dir_count += len(directories)
        files_in_dir = len(files)
        file_count += files_in_dir

        if files_in_dir > max_files:
            max_files = files_in_dir

        all_files.extend(os.path.join(root, f) for f in files)

    fm = FileMetric(tsuid=None, dircount=dir_count, filecount=file_count, maxfiles=max_files, files=all_files)

    LOGGER.debug(fm)

    return fm


def get_csv_length(path):
    """
    Get the number of lines of the specified file

    :param path: path to the file to analyse
    :type path: str

    :return: the number of lines in the csv
    :rtype: int
    """
    with open(path) as filename:
        return len(list(filename.readlines()))


# Prints the logger to display
log_to_stdout(LOGGER)


def cleanup_folder(path):
    """
    This method removes the path and its content from disk if the path is inside /tmp folder
    :param path: path to remove
    """
    try:
        if not path.startswith("/tmp"):
            raise ValueError("You try to remove a path not in /tmp")
        shutil.rmtree(path)
    except Exception:
        LOGGER.warning("Path %s not removed" % path)
    else:
        LOGGER.debug("Path %s removed" % path)


class Testexport_ts(TestCase):
    """
    Test of the export_ts package
    """

    def setUp(self):
        self.portfolio_tsuids = IkatsApi.ds.read("Portfolio")['ts_list']
        self.portfolio_metadata = [get_metadata(tsuid, '{fid}') for tsuid in self.portfolio_tsuids]

    def test_export_multi_process(self):
        """
        Run multiprocess version
        Portfolio dataset with folder for each fid => 14 folders 1 in each
        """
        pattern = "/{metric}/{fid}.csv"
        destination = "/tmp/tests/test_write_multi"

        cleanup_folder(destination)

        status = export_ts(ds_name="Portfolio", pattern=pattern, destination_path=destination,
                             multi_process=False, ok_to_overwrite=False)

        LOGGER.debug(status)

        fm = count_dirs_and_files(destination)
        self.portfolio_compare(fm, pattern=destination + pattern)

        cleanup_folder(destination)

    def test_export_single_process(self):
        """
        Single-process version
        """
        pattern = "/{metric}/{fid}.csv"
        destination = "/tmp/tests/test_write_single"

        cleanup_folder(destination)

        status = export_ts(ds_name="Portfolio", pattern=pattern, destination_path=destination,
                             multi_process=False, ok_to_overwrite=False)

        LOGGER.debug(status)

        fm = count_dirs_and_files(destination)
        self.portfolio_compare(fm, pattern=destination + pattern)

        cleanup_folder(destination)

    def test_export_single_time_series(self):
        """
        Test getting a time series from a tsuid and writing to csv
        Use portfolio dataset each of which has 48 points
        Thus check if 48 + 1 for header lines in csv
        One CSV file in per directory
        """
        pattern = "/{metric}/{fid}.csv"
        destination = "/tmp/tests/test_write_individual"

        cleanup_folder(destination)

        for tsuid in self.portfolio_tsuids:
            metadata = get_metadata(tsuid=tsuid, pattern=pattern)
            filled_pattern = pattern.format(**metadata)
            path = create_directory(destination_path=destination, pattern=filled_pattern, preexisting_files=set())
            fetch_and_write_time_series(path=path, tsuid=tsuid)

        fm = count_dirs_and_files(destination)
        self.portfolio_compare(fm, pattern=destination + pattern)

        cleanup_folder(destination)

    def test_export_single_folder(self):
        """
        Single-process version
        Multiple CSV files in a same directory
        """
        pattern = "/{qual_nb_points}/{fid}.csv"
        destination = "/tmp/tests/test_write_single_folder"

        cleanup_folder(destination)

        status = export_ts(ds_name="Portfolio", pattern=pattern, destination_path=destination,
                             multi_process=False, ok_to_overwrite=False)

        LOGGER.debug(status)

        fm = count_dirs_and_files(destination)
        expected_fm = FileMetric(tsuid=None, filecount=14, dircount=1, maxfiles=14, files=[])
        self.compare_file_metrics(obtained_fm=fm, expected_fm=expected_fm)

        cleanup_folder(destination)

    def test_default_destination(self):
        """
        Test that when no default destination is provided that /tmp/{ds_name} is used
        """
        pattern = "/{metric}/{fid}.csv"
        default_destination = "/tmp/portfolio"

        cleanup_folder(default_destination)

        status = export_ts(ds_name="Portfolio", pattern=pattern,
                             multi_process=False, ok_to_overwrite=False)

        LOGGER.debug(status)

        fm = count_dirs_and_files(default_destination)
        self.portfolio_compare(fm, pattern=default_destination + pattern)

        cleanup_folder(default_destination)

    def test_malformed_destination_path(self):
        """
        If destination path does not begin with a forward slash raise a ValueError
        """
        with self.assertRaises(ValueError):
            export_ts(ds_name="Portfolio", pattern="/error/(fid}.csv", destination_path="tmp/tests/malformed",
                        multi_process=False, ok_to_overwrite=False)

    def test_permission_denied_error(self):
        """
        if we do not have permission to write to a folder then raise a PermissionError
        """
        with self.assertRaises(PermissionError):
            export_ts(ds_name="Portfolio", pattern="/error/(fid}.csv", destination_path="/root",
                        multi_process=False, ok_to_overwrite=False)

    def test_fail_to_overwrite(self):
        """
        Call twice to same folder. Second should fail because ok_to_overwrite is False
        """
        pattern = "/{qual_nb_points}/{fid}.csv"
        destination = "/tmp/tests/test_fail_overwrite"

        cleanup_folder(destination)

        status = export_ts(ds_name="Portfolio", pattern=pattern, destination_path=destination,
                             multi_process=False, ok_to_overwrite=False)

        LOGGER.debug(status)
        with self.assertRaises(ValueError):
            export_ts(ds_name="Portfolio", pattern=pattern, destination_path=destination, multi_process=False)
        cleanup_folder(destination)

    def test_overwrite(self):
        """
        Call twice to same folder. If ok_to_overwrite=True should work
        """
        pattern = "/{qual_nb_points}/{fid}.csv"
        destination = "/tmp/tests/test_success_overwrite"

        cleanup_folder(destination)

        status = export_ts(ds_name="Portfolio", pattern=pattern, destination_path=destination,
                             multi_process=False, ok_to_overwrite=False)

        LOGGER.debug(status)

        export_ts(ds_name="Portfolio", pattern=pattern, destination_path=destination,
                    multi_process=False, ok_to_overwrite=True)

        cleanup_folder(destination)

    def test_make_dirs_pattern_slash(self):
        """
        Test that multiple combinations of:
            - patterns (with and w/o leading slash)
            - destination paths (with and without trailing slash
        can be combined
        """

        patterns = ["/{metric}/{fid}.csv", "{metric}/{fid}.csv"]
        destinations = ["/tmp/tests/test_join", "/tmp/tests/test_join/"]

        metadata_dict = {}

        for pattern, destination, tsuid in product(patterns, destinations, self.portfolio_tsuids):

            try:
                metadata = metadata_dict.get((tsuid, pattern), None)

                if metadata is None:
                    metadata = get_metadata(tsuid=tsuid, pattern=pattern)
                    metadata_dict[(tsuid, pattern)] = metadata

                filled_pattern = pattern.format(**metadata)
                create_directory(destination_path=destination, pattern=filled_pattern)
            except Exception as e:
                self.fail("Raised Exception unexpectedly!%s" % e)

            cleanup_folder(destination)

    def test_pattern_clashes_csv(self):
        """
        A pattern might be legally formed but fail to provide a unique name
        for each csv. Each Portfolio timeseries has 48 points so by using the pattern below
        they will all map to the same file.
        """

        pattern = "/fid/{qual_nb_points}.csv"
        destination = "/tmp/tests/test_clash_csv"
        cleanup_folder(destination)

        with self.assertRaises(ValueError):
            status = export_ts(ds_name="Portfolio", pattern=pattern, destination_path=destination,
                                 multi_process=False, ok_to_overwrite=True)
            LOGGER.debug(status)

        cleanup_folder(destination)

    def test_fid_in_md_if_requested(self):
        """
        If 'fid' is requested in the pattern ensure it is in the keys of the metadata
        """
        has_fid_pattern = "{fid}"

        for tsuid in self.portfolio_tsuids:
            metadata = get_metadata(tsuid, has_fid_pattern)
            self.assertTrue("fid" in metadata.keys())

    def test_incorrect_metadata(self):
        """
        Raises key error if metadata not found (can't be expanded in pattern)
        """
        with self.assertRaises(KeyError):
            export_ts(ds_name='Portfolio', pattern="/{unknown_metadata}/{unknown_metadata2}.csv",
                        multi_process=False, ok_to_overwrite=True)

    def compare_file_metrics(self, expected_fm, obtained_fm):
        """
        Take two file metrics about the directory structure and see that they are equivalent

        :param expected_fm: Expected metrics used as reference
        :param obtained_fm: Obtained metrics to compare with

        :type expected_fm: FileMetric
        :type obtained_fm: FileMetric
        """
        self.assertEqual(expected_fm.filecount, obtained_fm.filecount)
        self.assertEqual(expected_fm.dircount, obtained_fm.dircount)
        self.assertEqual(expected_fm.maxfiles, obtained_fm.maxfiles)

    def portfolio_compare(self, fm, pattern):
        """
        Common values for self.compare_file_metrics with portfolio dataset

        :param fm:
        :param pattern:

        :type pattern: str
        :type fm: FileMetric

        """
        expected_fm = FileMetric(tsuid=None, dircount=len(self.portfolio_tsuids), filecount=len(self.portfolio_tsuids),
                                 maxfiles=1,
                                 files=[])
        self.compare_file_metrics(expected_fm, fm)

        all_paths = {}

        LOGGER.debug(self.portfolio_metadata)

        for metadata in self.portfolio_metadata:

            try:
                filled_pattern = pattern.format(**metadata)
                all_paths[filled_pattern] = metadata
            except KeyError:
                LOGGER.debug("pattern " + pattern)
                LOGGER.debug("keys: " + ", ".join(metadata.keys()))
                LOGGER.debug(metadata)
                raise

        for path, md in all_paths.items():
            LOGGER.debug("Path to find: " + path)
            self.assertEqual(path in all_paths.keys(), True)
            self.assertEqual(get_csv_length(path), int(md['qual_nb_points']) + 1)


if __name__ == '__main__':
    unittest.main()
