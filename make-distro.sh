#!/bin/bash
set -e
version=$(grep -A1 "hammer-bench" pom.xml | grep -ioh "[0-9.]*")
mvn clean install
DIST=hammer-bench-$version
rm -rf $DIST 
mkdir $DIST 
cp -r scripts/* $DIST/ 
cp master.properties $DIST/
cp slave.properties $DIST/
cp target/hammer-bench-0.1.0-jar-with-dependencies.jar $DIST/hammer-bench.jar
echo "Version: $version" > $DIST/VERSION
echo "Built: `date`" >> $DIST/VERSION
tar czf $DIST.tgz $DIST
rm -rf $DIST
scp $DIST.tgz repo.hops.works:/opt/repository/master/hammer-bench

