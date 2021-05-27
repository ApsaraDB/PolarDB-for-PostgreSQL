#!/bin/bash

PGHOME=$HOME/pghome
PGBIN=$PGHOME/bin
CODEHOME=$PWD

set -e

# PostgreSQL
make clean

# extensions
cd $CODEHOME/contrib && make clean

echo "============> Enjoy coding! <==============="
echo "You could add $PGBIN to your PATH"
