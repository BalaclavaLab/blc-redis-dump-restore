# Overview

BLC Redis dump restore utility.

## Getting started

### Docker

* `docker run --rm -ti balaclavalab/blc-redis-dump-restore:0.1.4 -f redis://localhost/1 -t redis://localhost/2`

### Docker (building and running)

* Build with `./gradlew dockerBuildImage`
* Open build image `docker run --entrypoint bash -ti <imageId>`
* Use `/redis-dump-restore-<version>/bin/blc-redis-dump-restore` 

### Gradle

* Build with `./gradlew installDist`
* Go to `./blc-redis-dump-restore/build/install/fblc-redis-dump-restore/bin`
* Use `./blc-redis-dump-restore` 

### Examples:

* `./blc-redis-dump-restore -f redis://localhost/1 -t redis://localhost/2`
* `./blc-redis-dump-restore -m user#* -f redis://localhost/1 -t redis://localhost/2`

### Usage reference

```
usage: blc-redis-dump-restore [-f <arg>] [-l <arg>] [-m <arg>] [-t <arg>]
BLC Redis dump restore utility
 -f,--uriFrom <arg>     Redis from (e.g. redis://localhost/1)
 -t,--uriTo <arg>       Redis to (e.g. redis://localhost/2)
 -m,--scanMatch <arg>   Scan Match (default: *)
 -l,--scanLimit <arg>   Scan Limit (default: 5000)
```