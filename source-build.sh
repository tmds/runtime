#!/bin/sh

./build.sh /p:NoPgoOptmize=true /p:DotNetBuildFromSource=true --outputrid banana.10-x64 /p:RuntimeOS=linux -v diag

