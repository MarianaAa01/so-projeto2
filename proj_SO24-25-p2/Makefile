CC = gcc

# Para mais informações sobre as flags de warning, consulte a informação adicional no lab_ferramentas
CFLAGS = -g -O0 -std=c17 -D_POSIX_C_SOURCE=200809L -I. \
		 -Wall -Wextra \
		 -Wcast-align -Wconversion -Wfloat-equal -Wformat=2 -Wnull-dereference -Wshadow -Wsign-conversion -Wswitch-enum -Wundef -Wunreachable-code -Wunused \
		 -pthread
# -fsanitize=address -fsanitize=undefined


ifneq ($(shell uname -s),Darwin) # if not MacOS
	CFLAGS += -fmax-errors=5
endif

all: src/server/kvs src/client/client

src/server/kvs: src/common/protocol.h src/common/constants.h src/server/main.c src/server/operations.o src/server/kvs.o src/server/io.o src/server/parser.o src/common/io.o
	$(CC) $(CFLAGS) -o $@ src/server/main.c src/server/operations.o src/server/kvs.o src/server/io.o src/server/parser.o src/common/io.o


# ...existing code...

src/client/client: src/common/protocol.h src/common/constants.h src/client/main.c src/client/api.o src/client/parser.o src/common/io.o
	$(CC) $(CFLAGS) -o $@ src/client/main.c src/client/api.o src/client/parser.o src/common/io.o

# ...existing code...

%.o: %.c %.h
	$(CC) $(CFLAGS) -c ${@:.o=.c} -o $@

clean:
	rm -f src/common/*.o src/client/*.o src/server/*.o src/server/core/*.o src/server/kvs src/client/client src/client/client_write
format:
	@which clang-format >/dev/null 2>&1 || echo "Please install clang-format to run this command"
	clang-format -i src/common/*.c src/common/*.h src/client/*.c src/client/*.h src/server/*.c src/server/*.h

re:
	@make clean
	@make

lldb_client:
	lldb ./src/client/client 100 /tmp/apagar

client:
	./src/client/client 100 /tmp/apagar

lldb_server:
	lldb ./src/server/kvs src/server/jobs 10 10 /tmp/apagar

server:
	./src/server/kvs src/server/jobs 10 10 /tmp/apagar

clean_fds:
	rm /tmp/apagar /tmp/notif100 /tmp/req100 /tmp/resp100
