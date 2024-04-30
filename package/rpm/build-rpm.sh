#!/bin/bash

RPM_MACROS=$HOME/.rpmmacros
TOP_DIR=`pwd`/.self_rpm_create

release=${1:-$(date +"%Y%m%d%H%M%S")}

rm -f $RPM_MACROS
echo "%_topdir $TOP_DIR" > $RPM_MACROS
echo "%_release $release" >> $RPM_MACROS

rm -rf $TOP_DIR
RELEASE=$release rpmbuild -bb --buildroot $TOP_DIR/BUILD PolarDB.spec;st=$?

if [ "$st" != 0 ]; then
    exit $st
fi

cp -rf $TOP_DIR/RPMS/* .
