"""
Tests of Export Data
"""
import os
import shutil
import unittest
from unittest import TestCase

from collections import namedtuple

import logging

from ikats.core.resource.api import IkatsApi
from ikats.algo.export_ts.export_ts import export_ts, get_metadata, LOGGER


def log_to_stdout(logger_to_use):
    """
    Allow to print some loggers to stdout

    :param logger_to_use: the LOGGER object to redirect to stdout
    """

    logger_to_use.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(funcName)s:%(message)s')
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

    fm = FileMetric(tsuid=None, dir_count=dir_count, file_count=file_count, max_files=max_files, files=all_files)

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


def cleanup_folder(path):
    """
    This method removes the path and its content from disk
    :param path: path to remove relative to TSDATA
    """
    try:
        shutil.rmtree(path)
    except Exception:
        LOGGER.warning("Path %s not removed", path)
    else:
        LOGGER.debug("Path %s removed", path)


logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("ikats.core.resource.client.rest_client").setLevel(logging.WARNING)
FileMetric = namedtuple('FileMetric', 'tsuid dir_count file_count max_files files')

# Prints the logger to display
log_to_stdout(LOGGER)


class TestExportTS(TestCase):
    """
    Test of the export_ts package
    """

    def setUp(self):
        os.environ["TSDATA"] = os.path.realpath("./tests_export")
        # Review#499 : you are not supposed to use real data !
        self.portfolio_tsuids = IkatsApi.ds.read("Portfolio")['ts_list']
        self.portfolio_metadata = [get_metadata(tsuid, '{fid}') for tsuid in self.portfolio_tsuids]

    def test_nominal(self):
        """
        # Review#499 : why single process ?
        Single-process version
        """
        pattern = "{metric}/{fid}.csv"
        csv_output_path = export_ts(ds_name="Portfolio", pattern=pattern)

        fm = count_dirs_and_files(csv_output_path)
        self.portfolio_compare(csv_output_path, fm, pattern=pattern, expected_values={
            "dir_count": len(self.portfolio_tsuids),
            "file_count": len(self.portfolio_tsuids),
            "max_files": 1
        })
        cleanup_folder(csv_output_path)

    def test_no_folder(self):
        """
        All CSV written in the root directory
        """
        pattern = "{fid}.csv"

        csv_output_path = export_ts(ds_name="Portfolio", pattern=pattern)

        fm = count_dirs_and_files(csv_output_path)
        self.portfolio_compare(csv_output_path, fm, pattern=pattern, expected_values={
            "dir_count": 0,
            "file_count": len(self.portfolio_tsuids),
            "max_files": len(self.portfolio_tsuids)
        })
        cleanup_folder(csv_output_path)

    def test_single_folder(self):
        """
        All CSV written in a single directory
        """
        pattern = "{qual_nb_points}/{fid}.csv"

        csv_output_path = export_ts(ds_name="Portfolio", pattern=pattern)

        fm = count_dirs_and_files(csv_output_path)
        self.portfolio_compare(csv_output_path, fm, pattern=pattern, expected_values={
            "dir_count": 1,
            "file_count": len(self.portfolio_tsuids),
            "max_files": len(self.portfolio_tsuids)
        })
        cleanup_folder(csv_output_path)

    def test_pattern_clashes_csv(self):
        """
        A pattern might be legally formed but fail to provide a unique name
        for each csv. Each Portfolio timeseries has 48 points so by using the pattern below
        they will all map to the same file.
        """

        pattern = "/{ds}.csv"

        with self.assertRaises(ValueError):
            csv_output_path = export_ts(ds_name="Portfolio", pattern=pattern)
            # Review#499 : following lines useless because exception raised in export_ts above
            fm = count_dirs_and_files(csv_output_path)
            self.portfolio_compare(csv_output_path, fm, pattern=pattern, expected_values={
                "dir_count": 0,
                "file_count": len(self.portfolio_tsuids),
                "max_files": len(self.portfolio_tsuids)
            })

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
            export_ts(ds_name='Portfolio', pattern="/{unknown_metadata}/{unknown_metadata2}.csv")

    def compare_file_metrics(self, expected_fm, obtained_fm):
        """
        Take two file metrics about the directory structure and see that they are equivalent

        :param expected_fm: Expected metrics used as reference
        :param obtained_fm: Obtained metrics to compare with

        :type expected_fm: FileMetric
        :type obtained_fm: FileMetric
        """
        self.assertEqual(expected_fm.file_count, obtained_fm.file_count)
        self.assertEqual(expected_fm.dir_count, obtained_fm.dir_count)
        self.assertEqual(expected_fm.max_files, obtained_fm.max_files)

    def portfolio_compare(self, root_path, fm, pattern, expected_values):
        """
        Common values for self.compare_file_metrics with portfolio dataset

        :param root_path:
        :param fm:
        :param pattern:
        :param expected_values:

        :type root_path: str
        :type pattern: str
        :type fm: FileMetric
        :type expected_values: dict

        """
        expected_fm = FileMetric(tsuid=None, dir_count=expected_values['dir_count'],
                                 file_count=expected_values["file_count"],
                                 max_files=expected_values["max_files"],
                                 files=[])
        self.compare_file_metrics(expected_fm, fm)

        all_paths = {}

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
            self.assertEqual(path in all_paths.keys(), True)
            self.assertEqual(get_csv_length("%s/%s" % (root_path, path)), int(md['qual_nb_points']) + 1)


if __name__ == '__main__':
    unittest.main()
