#!/bin/bash

BLOB_PATH="wasb://ambcluster-2018-07-05t14-43-36-447z@ambcluster.blob.core.windows.net/temp/saint_marche/out"

DATE_WITH_TIME=$(/usr/bin/hive -S -e "select max(localtimestamp) from raw_gcat.processlog_saint_marche;")

/usr/bin/hive -S -e "load data inpath '$BLOB_PATH/gcat_saint_marche_out.csv' overwrite into table raw_gcat.saint_marche PARTITION(localtimestamp='$DATE_WITH_TIME');"

## Load data in pr_sellout DB
/usr/bin/hive -S -f ~/gcat/saint_marche/load_core_saint_marche.hql

