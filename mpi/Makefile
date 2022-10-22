CC = gcc
MPICC = mpicc
CFLAGS = -std=gnu99
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
	$(CC) $(CFLAGS) $< -o $@

$(BUILD_DIR)/client.exe: $(SRC_DIR)/client.c $(DEPS)
	export PATH=${PATH}:/usr/lib64/openmpi/bin:/usr/lib64/mpich-3.2/bin
	$(MPICC) $(CFLAGS) $< -o $@

#---------------------------------------------------------------

ADDR = 127.0.0.1
PORT = 12346

SIZEMB = 64

MPISLOTS = 2

cli:
	export PATH=${PATH}:/usr/lib64/openmpi/bin:/usr/lib64/mpich-3.2/bin
	mpiexec --allow-run-as-root -n $(MPISLOTS) ./client.exe $(ADDR) $(PORT)
host:
	mkdir -p logs
	./host.exe $(PORT) $(SIZEMB)


#---------------------------------------------------------------

clean:
	rm -rf $(BUILD_DIR)/*.exe
	rm -f logs/*

#---------------------------------------------------------------