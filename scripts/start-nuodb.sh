#!/bin/bash

NUODB_ROOT="/opt/nuodb"

$NUODB_ROOT/etc/nuoagent start

java -jar $NUODB_ROOT/jar/nuodbmanager.jar --broker localhost --password bird --command "start process sm host localhost database tsdb archive /var/lib/nuodb/tsdb/"

java -jar $NUODB_ROOT/jar/nuodbmanager.jar --broker localhost --database tsdb --password bird --command "start process te host localhost"
