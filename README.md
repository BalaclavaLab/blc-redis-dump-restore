# Overview

BLC Redis dump restore utility.

## Getting started

### Gradle

* Build with `./gradlew installDist`
* Go to `./blc-redis-dump-restore/build/install/fblc-redis-dump-restore/bin`
* Use `./blc-redis-dump-restore` 

### Docker

* Build with `./gradlew dockerBuildImage`
* Open build image `docker run --entrypoint bash -ti 05cbe0eed16c`
* Use `/redis-dump-restore-<version>/bin/blc-redis-dump-restore` 

### Examples:

* `./blc-redis-dump-restore -m interest#* -f redis://redis-interests001.mint.internal/1 -t redis://localhost/15`

### Usage reference

```
usage: blc-redis-dump-restore [-f <arg>] [-l <arg>] [-m <arg>] [-t <arg>]
BLC Redis dump restore utility
 -f,--uriFrom <arg>     Redis from (e.g.
                        redis://redis-interests001.mint.internal/1)
 -l,--scanLimit <arg>   Scan Limit (default: 5000)
 -m,--scanMatch <arg>   Scan Match (default: *)
 -t,--uriTo <arg>       Redis to (e.g. redis://localhost/15)
```