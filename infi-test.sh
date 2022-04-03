#!/bin/bash
# Recommend syntax for setting an infinite while loop
set -e
while :
do
	/opt/optane/CargoTarget/release/lightning-bench --file amd runtime --stride 4 -c -l 26
	#lldb --batch  -o run -f /opt/optane/CargoTarget/release/lightning-bench -- --file amd runtime --stride 4 -c -l 26
done
#lldb --batch  -o run -f /opt/optane/CargoTarget/debug/deps/lightning-4f3dc7ee5c8b0c6c -- ptr_map
