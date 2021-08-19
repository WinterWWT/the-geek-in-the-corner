.PHONY: clean

CFLAGS  := -Wall -g
#-Werror
LD      := gcc
LDLIBS  := ${LDLIBS} -lrdmacm -libverbs -lpthread

APPS    := rdma-client rdma-server

all: ${APPS}

rdma-client: rdma-client-fun.o rdma-client.o murmurhash.o crc32.o
	${LD} -o $@ $^ ${LDLIBS}

rdma-server: rdma-server-fun.o rdma-server.o murmurhash.o crc32.o
	${LD} -o $@ $^ ${LDLIBS}

clean:
	rm -f *.o ${APPS}

