#!/bin/sh

# GTM start script for test

export PATH=/tmp/pgxc/bin:$PATH
export DATA=/tmp/pgxc/data/gtm

# -------------------------------
# starting active...
# -------------------------------
gtm_ctl -D ${DATA} -Z gtm status

