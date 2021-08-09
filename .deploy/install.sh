#!/bin/bash
set -x
set -e
set -u
set -C
# Add the unzip util to the container.
apt-get update -q
apt-get install -yq unzip
# Move the ant distribution into the PATH
mv .deploy/ant-v1.9.15.zip /usr/local/bin
cd /usr/local/bin
unzip -q ant-v1.9.15.zip
# Remove the binary distribution, now that we have an unzipped copy.
rm /usr/local/bin/ant-v1.9.15.zip
# Return to the working directory.
cd -
PATH=$PATH:/usr/local/bin/apache-ant-1.9.15/bin
# Install Ivy, resolve dependencies, and build OLTPBench.
ant bootstrap
ant resolve 2> /dev/null # There's a harmless warning message that clogs the console.
ant build
