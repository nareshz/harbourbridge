#!/bin/sh
if type mysql >/dev/null 2>&1; then
    echo "MySQL present."
else
    echo "MySQL not present."
fi