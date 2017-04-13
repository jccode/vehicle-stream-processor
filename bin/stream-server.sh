#!/bin/sh

BASEDIR=$(dirname $([ -L $0 ] && readlink -f $0 || echo $0))
MAIN_CLASS=com.suyun.vehicle.app.StreamProcessorApp
CLASSPATH=$BASEDIR/vehicle-stream-processor-1.0-SNAPSHOT.jar:$BASEDIR/dependency/*:$BASEDIR/resources
LOG_CONFIG=$BASEDIR/resources/logback.xml
ARGS="-Dlogback.configurationFile=${LOG_CONFIG}"

java $ARGS -cp $CLASSPATH $MAIN_CLASS
