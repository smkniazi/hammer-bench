#!/bin/bash

DIST=hammer-bench
rm -rf $DIST 
mkdir $DIST 
cp -r scripts/* $DIST/ 
cp master.properties $DIST/
cp slave.properties $DIST/
cp target/hammer-bench-1.0-SNAPSHOT-jar-with-dependencies.jar $DIST/hammer-bench.jar
tar czf $DIST.tgz $DIST
rm -rf $DIST
