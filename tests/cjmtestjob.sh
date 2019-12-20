#!/bin/bash
set -x
# Sleep
echo "##### HOST DETAILS #####"
hostname
date
pwd
echo "ARG = $1"
echo "########################"

if [ "$1" == "1001" ]; then
    echo "Sleeping 3 seconds"
    sleep 3
elif [ "$1" == "1002" ]; then
    echo "Sleeping 80 seconds"
    sleep 80
elif [ "$1" == "1003" ]; then
    echo "Sleeping 140 seconds"
    sleep 140
elif [ "$1" == "SHOULDFAIL" ]; then
    echo "Executing a command that does not exist: some_command_that_does_not_exist"
    some_command_that_does_not_exist
else
    echo "Argument has no treatment"
fi