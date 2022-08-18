#!/bin/bash
# Recommend syntax for setting an infinite while loop
set -e
export RUST_BACKTRACE=1
export RUSTFLAGS=-Zsanitizer=address RUSTDOCFLAGS=-Zsanitizer=address
while :
do
	current_date_time="`date "+%Y-%m-%d %H:%M:%S"`";
	echo $current_date_time;
	# lldb --batch  -o run -f  /opt/optane/CargoTarget/debug/deps/lightning-6906b0a7f192f421 -- ptr_map --test-threads=1
	cargo test parallel_with_resize --target x86_64-unknown-linux-gnu -- --test-threads=1
	#lldb --batch  -o run -f /opt/optane/CargoTarget/release/lightning-bench -- --file amd runtime --stride 4 -c -l 26
done
#lldb --batch  -o run -f /opt/optane/CargoTarget/debug/deps/lightning-4f3dc7ee5c8b0c6c -- ptr_map
