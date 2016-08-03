#!/bin/bash

DIR=/proj/networks/darst/hackathon/

echo $DIR
#chmod g+rwX $DIR
find $DIR -name .git -prune -or -type d -exec echo chmod g+s {} \;