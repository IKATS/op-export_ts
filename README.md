# op-export_ts

This operator allow to export a dataset to a set of CSV files ([see format below](#CSV-format) following a defined [path pattern](#Pattern-format).

The directory tree can be customized to define the name of the folders, subfolders and filenames based on timeseries metadata.

## Overview

### Input and parameters

The operator takes 1 input:

* **DSname**: The dataset name (string) to extract timeseries from

It also takes 1 parameter from the user:

* **Pattern**: The path pattern used to build the directory tree ([see pattern format below](#Pattern-format)).

### Outputs

The operator has 1 Output:

* **Path**: String indicating where the CSV files are located on container disk relative to TSDATA environment variable

## Details

### Pattern format

The path pattern is a string containing placeholders for specific metadata.

Placeholders are defined using curly brackets surrounding the keyword.

As an example, you could create a directory tree where:

* the first folder level is the number of points of the timeseries
* the second level is the maximum value of the timeseries
* the filename is composed of a `city` and `rank` metadata

The corresponding pattern could be: `{qual_nb_points}/{max_value}/{city}_{rank}.csv`

Reserved keywords are:

* `{fid}` for the functional identifier (the displayed name of the timeseries)
* `{DSname}` for the name of the dataset (specified as input)

### CSV format

The CSV format is composed of 2 columns delimited by the character `;`
Columns:

* Date (ISO-8601) (eg. 2018-01-01 12:34:56.789)
* Value (float)

### Limitations

* If the given *path pattern* produces 2 or more identical file path, the export is aborted.
* If the given *path pattern* can't be produced due to unknown metadata, the following pattern is used as fallback `{fid}.csv`
