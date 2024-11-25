all:
	bazel build -c dbg //:remote
dbg:
	LD_LIBRARY_PATH=/usr/local/lib gdb -tui bazel-bin/remote
