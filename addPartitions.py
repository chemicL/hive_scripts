#!/usr/bin/env python

import sys
import subprocess
import os

def pathsFromLsOutput(lsOutput):
    paths = lsOutput.split('\n')[1:-1]
    return paths

def getLastPathParts(paths):
    parts = [0] * len(paths)
    i = 0
    for path in paths:
        parts[i] = path.split()[-1].split('/')[-1]
        i = i + 1
    return parts

def addPartitions(tableName, pathBase, insertdates, dbName):
    statement = 'USE %s; ' % dbName;
    statement += 'ALTER TABLE %s ADD IF NOT EXISTS ' % tableName;
    for insertdate in insertdates:
        (year, month, day) = insertdate
        path = pathBase + '/%s/%s/%s' % (year, month, day)
        insertdateStr = '%s-%s-%s' % (year, month, day)
        statement += 'PARTITION (insertdate = \'%s\') LOCATION \'%s\' ' % (insertdateStr, path)
    print statement
    cmd = 'hive -e "%s"' % statement
    os.system(cmd)

def main():
    pathBase = sys.argv[1]
    tableName = sys.argv[2]
    dbName = 'default'
    if len(sys.argv) == 4:
        dbName = sys.argv[3]

    yearLs = subprocess.Popen(['hdfs', '-ls ' + pathBase], stdout=subprocess.PIPE).communicate()[0]
    yearPaths = pathsFromLsOutput(yearLs)
    years = getLastPathParts(yearPaths)

    insertdates = []

    for year in years:
        path = pathBase + '/' + str(year)
        monthLs = subprocess.Popen(['hdfs', '-ls ' + path], stdout=subprocess.PIPE).communicate()[0]
        monthPaths = pathsFromLsOutput(monthLs)
        months = getLastPathParts(monthPaths)

        for month in months:
            path = pathBase + '/' + str(year) + '/' + str(month)
            dayLs = subprocess.Popen(['hdfs', '-ls ' + path], stdout=subprocess.PIPE).communicate()[0]
            dayPaths = pathsFromLsOutput(dayLs)
            days = getLastPathParts(dayPaths)

            for day in days:
                insertdates.append([year, month, day])
        addPartitions(tableName, pathBase, insertdates, dbName)
        insertdates = []

if __name__ == "__main__":
        sys.exit(main())
