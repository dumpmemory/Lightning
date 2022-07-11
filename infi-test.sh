#!/bin/bash
# Recommend syntax for setting an infinite while loop
set -e
export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
while :
do
	cargo test map::ptr_map::ptr_map::parallel_with --target x86_64-unknown-linux-gnu -- --test-threads=1
	#lldb --batch  -o run -f /opt/optane/CargoTarget/release/lightning-bench -- --file amd runtime --stride 4 -c -l 26
done
#lldb --batch  -o run -f /opt/optane/CargoTarget/debug/deps/lightning-4f3dc7ee5c8b0c6c -- ptr_map
