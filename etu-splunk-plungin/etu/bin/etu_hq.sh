#!/bin/sh

export CLASSPATH=/mnt/hgfs/LinuxShare/etu-splunk-plugin.jar


/root/xm/jdk1.6.0_30/jre/bin/java etu.splunk.hbase.H4Splunk $@
