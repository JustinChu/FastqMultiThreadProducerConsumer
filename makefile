CC=g++
CFLAGS=-g -Wall -O3 -Wno-unused-function
BINDIR=/usr/local/bin

all:producerconsumer

producerconsumer:ProducerConsumer.cc concurrentqueue.h kseq_util.h kseq.h
		$(CC) $(CFLAGS) ProducerConsumer.cc -o $@ -fopenmp -std=c++11 -lz

install:all
		install producerconsumer $(BINDIR)

clean:
		rm -fr producerconsumer
