all:
	bazel build -c dbg --copt="-fsanitize=address" --linkopt="-fsanitize=address" //:remote
dbg:
	LD_LIBRARY_PATH=/usr/local/lib gdb -tui bazel-bin/remote
