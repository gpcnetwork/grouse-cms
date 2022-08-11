#!/bin/bash

# aws server
# pip3 install -r ~/environment/GROUSE/dep/requirements.txt --use-feature=2020-resolver

# windows laptop (can't install as admin)
$Env:Path # show path
$Env:Path += ";c:\users\xsm7f\appdata\roaming\python\python310" # may need to add user default lib path
pip3 install -r ./dep/requirements.txt --user
