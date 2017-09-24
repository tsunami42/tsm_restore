#   tsm_restore

##  Intro

Read single tsm file and export to stdout as `influx -import` file format.

A lite version of [`influx_inspect export`](https://github.com/influxdata/influxdb/tree/master/cmd/influx_inspect/export) command

##  Usage   

### Help

```
Usage of tsm_restore:
  -c    Compress the output in gzip
  -db string
        Database of tsm file (default "telegraf")
  -l int
        Limit keys to be read from index (default 100)
  -p string
        path for tsm file (default "a.tsm")
  -rp string
        RP of tsm file (default "default")
```

### Sample output

```
$ ./tsm_restore -p a.tsm -l 1

# DML
# CONTEXT-DATABASE:telegraf
# CONTEXT-RETENTION-POLICY:default
randset value=97.9296104805 1439856000000000000
randset value=25.3849066842 1439856100000000000
```

### Pipe to influx -import

```
# normal output
$ ./tsm_restore | influx -import -path /dev/stdin -pps 10000

# compressed output
$ ./tsm_restore -c | influx -import -path /dev/stdin -compressed -pps 10000
```