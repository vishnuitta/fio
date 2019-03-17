#!/bin/bash

run_top()
{
	top_logs_dir=$1
	top_outfile_ext=$2
	while [ 1 ]; do
#		script -a -c "top -b -d 2 -n 2" script.out.1
		top -b -d 2 -n 2 >> $top_logs_dir/top.$top_outfile_ext
		sleep 3
	done
}

run_iostat()
{
	iostat_logs_dir=$1
	iostat_outfile_ext=$2
	while [ 1 ]; do
		iostat -xt 2 2 >> $iostat_logs_dir/iostat.$iostat_outfile_ext
		sleep 2
	done
}

run_stats()
{
	run_top $1 $2 &
	top_pid=$!
	echo "top pid: $top_pid"

	run_iostat $1 $2 &
	iostat_pid=$!
	echo "iostat pid: $iostat_pid"
}

run_tests()
{
	./fio zfs-randwrite.conf
	du -h /tmp/test1.img
}

stop_stats()
{
	pkill -9 -P $top_pid
	kill -9 $top_pid

	pkill -9 -P $iostat_pid
	kill -9 $iostat_pid
}

if [ $# != 2 ]; then
	echo "Usage: run_test.sh logs_dir outfile_ext"
	exit
fi
logs_dir=$1
outfile_ext=$2

echo "Doing output to $logs_dir/top.$outfile_ext and $logs_dir/iostat.$outfile_ext"

run_stats $logs_dir $outfile_ext

run_tests

stop_stats

