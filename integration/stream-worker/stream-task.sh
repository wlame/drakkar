#!/bin/sh
# One task of the stream worker: wait the given seconds, then print the
# name of the task's group as one JSON line.
#   stream-task <seconds> <group name>
sleep "$1"
printf '{"group": "%s"}\n' "$2"
