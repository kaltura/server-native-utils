# Gzip logs utilities

Tools for working with segmented-gzip log files. 
Segmented-gzip files are files in which the gzip stream is periodically closed, making it possible to start reading the file from various offsets.
This is unlike regular gzip files, which always have to be read from the beginning.

## log_compressor

Writes segmented-gzip files, has two working modes -
* Daemon - read logs from a unix datagram socket / pipe (fifo), compresses them and writes to disk.
* Offline - read logs from a file/stdin and write to a file/stdout (similar to the gzip utility)

## ztail

Similar to the tail utility - reads lines from the end of a segmented-gzip file, supports 'follow' mode.

## zbingrep

Grep a segmented-gzip file by performing binary search (assumes the file is sorted by time)

## zgrepindex

Create an index of a segmented-gzip - returns of mapping of file offsets -> time stamps.

## zblockgrep

Grep gzip files/file ranges containing log messages that may span across multiple lines. Unlike the standard grep utility that works with 'lines', this tool works with 'blocks'.
