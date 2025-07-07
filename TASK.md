# Task:
We are going to fix a bug.


The app creates a managed repository of dataset files - here is an example: '/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs'.

## Problem

 There is a bug in how it generates paths.

`capability` files are stored in <root>/cabability/
`data` files are stored under their quality classification in <root>/<quality>/<county>

The problem is that there are some other files that are neither that are being filed in <root>/no-quality. These have to be correctly filed.

## Solution

Refactor the code as follows:

1. Ensure the file in '/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs/no-quality/midas-open_uk-daily-temperature-obs_dv-202507_station-metadata.csv/no-station/midas-open_uk-daily-temperature-obs_dv-202507_station-metadata.csv' is filed as '/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs/station-metadata/midas-open_uk-daily-temperature-obs_dv-202507_station-metadata.csv'

2.Ensure that the station log files in '/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs/no-quality/change_log_station_files' are stored in '/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs/station-log-files' without storing them in the redundant fodler they are in

3. Ensure that '/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs/no-quality/midas-open_uk-daily-temperature-obs_dv-202507_change_log.txt/no-station/midas-open_uk-daily-temperature-obs_dv-202507_change_log.txt' is stored in '/Users/richardlyon/Library/Application Support/midas-fetcher/cache/uk-daily-temperature-obs'

All using the deinfed cache root - these hardcoded paths are given as example only.

Generate tests to verify.
Reorganise the existing dataset folders to comply.
