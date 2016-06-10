#!/usr/bin/env bash

#Clean the modules which will be backed up.
mvn clean
cd ..

#Next we create the backup directory
mkdir -p backups
cd backups
export dir=$(date -d "today" +"%Y-%m-%d-%H-%M-%S")
mkdir $dir
cd $dir
cp -r ../../debs2016 .
cd ..
#Zip the backup
zip -r $dir.zip $dir
#Do the final cleaning
rm -rf $dir
