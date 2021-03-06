"""
Copyright 2018-2019 CS Systèmes d'Information

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
import os
import shutil
import unittest
from unittest import TestCase

from collections import namedtuple

import logging

from ikats.core.resource.api import IkatsApi
from ikats.algo.export_ts.export_ts import export_ts, get_metadata, LOGGER
from ikats.core.resource.client.temporal_data_mgr import DTYPE


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


def _gen_ts(fid, data):
    try:
        tsuid = IkatsApi.fid.tsuid(fid=fid)
        IkatsApi.ts.delete(tsuid=tsuid, no_exception=True)
    except ValueError:
        # No TS to delete
        pass

    result = IkatsApi.ts.create(fid=fid, generate_metadata=True, data=data)
    qual_nb_points = len(data)
    qual_ref_period = (data[-1][0] - data[0][0]) / qual_nb_points
    IkatsApi.md.create(tsuid=result['tsuid'], name="qual_ref_period", value=qual_ref_period, data_type=DTYPE.number)
    IkatsApi.md.create(tsuid=result['tsuid'], name="qual_nb_points", value=qual_nb_points, data_type=DTYPE.number)
    return result['tsuid']


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

    @classmethod
    def setUpClass(cls):
        os.environ["TSDATA"] = os.path.realpath("./tests_export")

        test_tsuid_list = [
            _gen_ts(fid="FID_TS_1", data=[
                [1e12, 5.0],
                [1e12 + 1000, 6.2],
                [1e12 + 2000, 6.0],
                [1e12 + 3600, 8.0],
                [1e12 + 4000, -15.0],
                [1e12 + 5000, 2.0],
                [1e12 + 6000, 6.0],
                [1e12 + 7000, 3.0],
                [1e12 + 8000, 2.0],
                [1e12 + 9000, 42.0],
                [1e12 + 10000, 8.0],
                [1e12 + 11000, 8.0],
                [1e12 + 12000, 8.0],
                [1e12 + 13000, 8.0]
            ]),
            _gen_ts(fid="FID_TS_2", data=[
                [1e12, 5.0],
                [1e12 + 1000, 8.2],
                [1e12 + 2000, 6.6],
                [1e12 + 3600, -8.0],
                [1e12 + 4000, -14.0],
                [1e12 + 5000, 42.0],
                [1e12 + 6000, 26.0],
                [1e12 + 7000, 10023.0],
                [1e12 + 8000, 22.0],
                [1e12 + 9000, 42.0],
                [1e12 + 10000, 8.587678],
                [1e12 + 11000, 8.0],
                [1e12 + 12000, 8.0],
                [1e12 + 13000, 8.0]
            ]),
            _gen_ts(fid="FID_TS_3", data=[
                [1e12, 5.10],
                [1e12 + 1000, 8.2],
                [1e12 + 2000, 6.6],
                [1e12 + 3600, -8.0],
                [1e12 + 4000, -14.0],
                [1e12 + 5000, 42.2],
                [1e12 + 6000, 26.0],
                [1e12 + 7000, 10023.0],
                [1e12 + 8000, 22.0],
                [1e12 + 9000, 42.0],
                [1e12 + 10000, 8.587678],
                [1e12 + 11000, 8.0],
                [1e12 + 12000, 8.7],
                [1e12 + 13000, 8.0]
            ]),
        ]
        IkatsApi.ds.create("TEST_EXPORT_DATA", description="Test dataset used for export_data operator",
                           tsuid_list=test_tsuid_list)

        cls.ds_test_tsuid_list = test_tsuid_list
        cls.ds_test_metadata = [get_metadata(tsuid) for tsuid in cls.ds_test_tsuid_list]

    @classmethod
    def tearDownClass(cls):
        IkatsApi.ds.delete(ds_name="TEST_EXPORT_DATA", deep=True)

    def test_no_folder(self):
        """
        All CSV written in the root directory
        """
        pattern = "{fid}.csv"

        csv_output_path = export_ts(ds_name="TEST_EXPORT_DATA", pattern=pattern)
        out_path = os.environ.get('TSDATA') + '/' + csv_output_path

        fm = count_dirs_and_files(out_path)
        self.dataset_compare(out_path, fm, pattern=pattern, expected_values={
            "dir_count": 0,
            "file_count": len(self.ds_test_tsuid_list),
            "max_files": len(self.ds_test_tsuid_list)
        })
        cleanup_folder(csv_output_path)

    def test_single_folder(self):
        """
        All CSV written in a single directory
        """
        pattern = "{qual_nb_points}/{fid}.csv"

        csv_output_path = export_ts(ds_name="TEST_EXPORT_DATA", pattern=pattern)
        out_path = os.environ.get('TSDATA') + '/' + csv_output_path

        fm = count_dirs_and_files(out_path)
        self.dataset_compare(out_path, fm, pattern=pattern, expected_values={
            "dir_count": 1,
            "file_count": len(self.ds_test_tsuid_list),
            "max_files": len(self.ds_test_tsuid_list)
        })
        cleanup_folder(csv_output_path)

    def test_pattern_clashes_csv(self):
        """
        A pattern might be legally formed but fail to provide a unique name for each csv.
        All time series will map to the same file.
        """

        pattern = "{DSname}.csv"

        with self.assertRaises(ValueError):
            export_ts(ds_name="TEST_EXPORT_DATA", pattern=pattern)

    def test_incorrect_metadata(self):
        """
        Raises key error if metadata not found (can't be expanded in pattern)
        """
        pattern = "/{unknown_metadata}/{unknown_metadata2}.csv"
        csv_output_path = export_ts(ds_name='TEST_EXPORT_DATA', pattern=pattern)
        out_path = os.environ.get('TSDATA') + '/' + csv_output_path

        fm = count_dirs_and_files(out_path)
        self.dataset_compare(out_path, fm, pattern="{fid}.csv", expected_values={
            "dir_count": 0,
            "file_count": len(self.ds_test_tsuid_list),
            "max_files": len(self.ds_test_tsuid_list)
        })
        cleanup_folder(csv_output_path)

    def test_fid_in_md_if_requested(self):
        """
        If 'fid' is requested in the pattern ensure it is in the keys of the metadata
        """
        for tsuid in self.ds_test_tsuid_list:
            metadata = get_metadata(tsuid)
            self.assertTrue("fid" in metadata.keys())

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

    def dataset_compare(self, root_path, fm, pattern, expected_values):
        """
        Common values for self.compare_file_metrics with TEST_EXPORT_DATA dataset

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

        for metadata in self.ds_test_metadata:

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
