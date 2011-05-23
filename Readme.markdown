# Overview

This gem is designed to sync an immutable directory to a specific directory on Amazon S3.  In principal it is similar to time machine for Mac.  We use this utility to create snapshots of our cassandra data.

# Installing

		gem install s3snapshot

# Operations

* Backup a directory
* Restore a specific snapshot time to a directory
* Restore latest complete snapshot to a directory
* List all prefixes
* List all times for prefixes
* Clean incomplete uploads (Use wisely, can delete a backup in progress)
* Perform rolling cleanup.  Can keep a user defined number of daily and weekly backups with user specified day.

# Algorithm

Below is a general outline on how the plugin was designed to work

## Snapshot path

All snapshot paths are of the format [prefix]/[snapshot utc time].  In our usage, we typically use the prefix of node+function, such as test-cass-west-1_cassandra

## Rolling cleanup

Capture the current time UTC, and truncate to 00:00 hours
Remove all incomplete backups before start time
Analyze all time stamps, if more than one backup is present per day, only keep the latest complete backup for that day
For all backups, if older than the max weekly, remove it , of between oldest weekly and oldest daily, only keep it if it falls on the day specified


# Notes

Occasionally a cleanup operation will miss a time stamp.  From my testing this appears to be due to the amazon eventual consistency, and the timestamp not being returned on a delim search.  On the next run it is usually deleted