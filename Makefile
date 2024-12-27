all:
	bazel build -c dbg //:replica_ut --copt=-fsanitize=address --linkopt=-fsanitize=address  --copt=-fdiagnostics-color=always --disk_cache=~/.cache/bazel
dbg:
	LD_LIBRARY_PATH=/usr/local/lib gdb -tui bazel-bin/remote
