#!/bin/bash
ant clean dist
hadoop-moonshot jar dist/TwitterHashtag.jar TwitterHashtag /data/olympictweets2016rio out /user/gr301/OlympicCountries.csv
hadoop-moonshot fs -getmerge out output.txt
less output.txt
