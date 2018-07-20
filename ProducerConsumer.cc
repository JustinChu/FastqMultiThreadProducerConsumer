/*
 * Example program for efficient multithread producer-consumer I/O with kseq
 */

#include <omp.h>
#include "concurrentqueue.h"
#include <zlib.h>
#ifndef KSEQ_INIT_NEW
#define KSEQ_INIT_NEW
#include "kseq.h"
KSEQ_INIT(gzFile, gzread)
#endif /*KSEQ_INIT_NEW*/
#include "kseq_util.h"

const static int threads = 64;

//still don't know the optimal size of this yet
const static size_t bulkSize = 32;
//still don't know the optimal size of this yet but must be greater than bulkSize
const static size_t maxQueueSize = threads * bulkSize;

void readLoadFinal(const char* filename) {
	moodycamel::ConcurrentQueue<kseq_t> workQueue(maxQueueSize);
	bool good = true;
#pragma omp parallel
	{
		kseq_t readBuffer[bulkSize];
		memset(&readBuffer, 0, sizeof(kseq_t) * bulkSize);
		if (omp_get_thread_num() == 0) {
			//file reading init
			gzFile fp;
			fp = gzopen(filename, "r");
			kseq_t *seq = kseq_init(fp);

			//per thread token
			moodycamel::ProducerToken ptok(workQueue);

			unsigned size = 0;
			while (kseq_read(seq) >= 0) {
				cpy_kseq(&readBuffer[size++], seq);
				if (bulkSize == size) {
                    //try to insert, if cannot queue is full
                    while (!workQueue.try_enqueue_bulk(ptok, readBuffer, size)) {
                    	//try to work
						if (kseq_read(seq) >= 0) {
//------------------------WORK CODE START---------------------------------------
							assert(seq->seq.s); //work
//------------------------WORK CODE END-----------------------------------------
						}
						else{
							break;
						}
                    }
					size = 0;
				}
			}
			//finish off remaining work
			for (unsigned i = 0; i < size; ++i) {
//------------------------WORK CODE START---------------------------------------
				assert(readBuffer[i].seq.l); //work
//------------------------WORK CODE END-----------------------------------------
			}
			if (workQueue.size_approx()) {
				moodycamel::ConsumerToken ctok(workQueue);
				//join in if others are still not finished
				while (workQueue.size_approx()) {
					size_t num = workQueue.try_dequeue_bulk(ctok, readBuffer,
							bulkSize);
					if (num) {
						for (unsigned i = 0; i < num; ++i) {
//------------------------WORK CODE START---------------------------------------
							assert(readBuffer[i].seq.l); //work
//------------------------WORK CODE END-----------------------------------------
						}
					}
				}
			}
#pragma omp atomic update
			good &= false;
			kseq_destroy(seq);
			gzclose(fp);
		} else {
			moodycamel::ConsumerToken ctok(workQueue);
			while (good) {
				if (workQueue.size_approx() >= bulkSize) {
					size_t num = workQueue.try_dequeue_bulk(ctok, readBuffer,
							bulkSize);
					if (num) {
						for (unsigned i = 0; i < num; ++i) {
//------------------------WORK CODE START---------------------------------------
							assert(readBuffer[i].seq.l); //work
//------------------------WORK CODE END-----------------------------------------
						}
					}
				}
			}
		}
	}
}

int main(int argc, char **argv) {
	omp_set_num_threads(threads);
	readLoadFinal(argv[1]);
}
