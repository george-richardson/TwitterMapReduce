#!/bin/bash
ant clean dist
hadoop-moonshot jar dist/TwitterTime.jar TwitterTime /data/olympictweets2016rio out
hadoop-moonshot fs -getmerge out output.txt
less output.txt
