CC = gcc
CFLAGS = -std=gnu99 -fopenmp
HFLAGS = -std=gnu99
_DEPS = lib.h
BUILD_DIR = .
SRC_DIR = src
LIB_DIR = ../common
DEPS = $(patsubst %,$(LIB_DIR)/%,$(_DEPS))
SERV = $(BUILD_DIR)/host.exe
CLI = $(BUILD_DIR)/client.exe

.PHONY: all cli host clean

all: $(CLI) $(SERV)

$(BUILD_DIR)/host.exe: $(LIB_DIR)/host.c $(DEPS)
	$(CC) $(HFLAGS) $< -o $@

$(BUILD_DIR)/client.exe: $(SRC_DIR)/client.c $(DEPS)
	$(CC) $(CFLAGS) $< -o $@

#---------------------------------------------------------------

ADDR = 127.0.0.1
PORT = 12346

SIZEMB = 128

cli:
	./client.exe $(ADDR) $(PORT)
host:
	mkdir -p logs
	./host.exe $(PORT) $(SIZEMB)


#---------------------------------------------------------------

clean:
	rm -rf $(BUILD_DIR)/*.exe
	rm -f logs/*

#---------------------------------------------------------------