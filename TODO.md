# ceda_fetcher

This is a fum app. A Tauri rust To do a very boring thing. It's going to download data from the CEDA website, but do it with a fun, high-tech Tauri front-end. This is to explore how to make Tauri apps.

In the left pane, there'll be a browser. It'll go onto the CETA website and show current data sets and their years. Probably cursor or something to select a set and download. On the right there'd be a map of Britain. Some sort of funky progress bar arrangement. It'll download the respective dataset. As the dataset is downloaded, it will appear on the map. There'll be opportunity to maintain different versions of the data set and different data sets

Things I think I'll need:

A command-line interface.
A CIDA client that can get pages and can download CSV files.
Some way of parsing pages to get relevant data out of them.
A way of getting available datasets.
A way of getting available years for a given data set.
A module for taking a manifest of data files and converting into a stream of target data files.
A structure for representing a given file for downloading
A module for managing an archive of data on the file base.
An orchestrator for getting files and downloading them in parallel.
A way of representing the status of the download given that it can last quite a long time
A Tauri app.
