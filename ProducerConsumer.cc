/*
 * Example program for efficient multithread producer-consumer I/O with kseq
 */

#include <omp.h>
#include "concurrentqueue.h"
#include <zlib.h>
#include <vector>
#ifndef KSEQ_INIT_NEW
#define KSEQ_INIT_NEW
#include "kseq.h"
KSEQ_INIT(gzFile, gzread)
#endif /*KSEQ_INIT_NEW*/
#include "kseq_util.h"

const static int s_threads = 64;

//still don't know the optimal size of this yet
const static size_t s_bulkSize = 32;
//still don't know the optimal size of this yet but must be greater than bulkSize
const static size_t s_maxQueueSize = s_threads * s_bulkSize;

void readLoadFinal(const char* filename) {
	unsigned numThreads = omp_get_num_threads();
	if (numThreads == 1) {
		gzFile fp;
		fp = gzopen(filename, "r");
		kseq_t *seq = kseq_init(fp);
		while (kseq_read(seq) >= 0) {
			//WORK
			assert(*seq);
		}
	} else {
		moodycamel::ConcurrentQueue<kseq_t> workQueue(numThreads * s_bulkSize);
		moodycamel::ConcurrentQueue<kseq_t> recycleQueue(
				numThreads * s_bulkSize * 2);
		bool good = true;
		typedef std::vector<kseq_t>::iterator iter_t;
		//fill recycleQueue with empty objects
		{
			std::vector<kseq_t> buffer(numThreads * s_bulkSize  * 2, kseq_t());
			recycleQueue.enqueue_bulk(
					std::move_iterator<iter_t>(buffer.begin()), buffer.size());
		}
#pragma omp parallel firstprivate(privObj)
		{
			std::vector<kseq_t> readBuffer(s_bulkSize);
			if (omp_get_thread_num() == 0) {
				//file reading init
				gzFile fp;
				fp = gzopen(filename, "r");
				kseq_t *seq = kseq_init(fp);

				//per thread token
				moodycamel::ProducerToken ptok(workQueue);

				//tokens for recycle queue
				moodycamel::ConsumerToken rctok(recycleQueue);
				moodycamel::ProducerToken rptok(recycleQueue);

				unsigned dequeueSize = recycleQueue.try_dequeue_bulk(rctok,
						std::move_iterator<iter_t>(readBuffer.begin()),
						s_bulkSize);
				while (dequeueSize == 0) {
					dequeueSize = recycleQueue.try_dequeue_bulk(rctok,
							std::move_iterator<iter_t>(readBuffer.begin()),
							s_bulkSize);
				}

				unsigned size = 0;
				while (kseq_read(seq) >= 0) {
					cpy_kseq(&readBuffer[size++], seq);
					if (dequeueSize == size) {
						//try to insert, if cannot queue is full
						while (!workQueue.try_enqueue_bulk(ptok,
								std::move_iterator<iter_t>(readBuffer.begin()),
								size)) {
							//try to work
							if (kseq_read(seq) >= 0) {
								//WORK
								assert(*seq);
							} else {
								break;
							}
						}
						//reset buffer
						dequeueSize = recycleQueue.try_dequeue_bulk(rctok,
								std::move_iterator<iter_t>(readBuffer.begin()),
								s_bulkSize);
						while (dequeueSize == 0) {
							//try to work
							if (kseq_read(seq) >= 0) {
								//WORK
								assert(*seq);
							} else {
								break;
							}
							dequeueSize = recycleQueue.try_dequeue_bulk(rctok,
									std::move_iterator<iter_t>(
											readBuffer.begin()), s_bulkSize);
						}
						size = 0;
					}
				}
				//finish off remaining work
				for (unsigned i = 0; i < size; ++i) {
					//WORK
					assert(readBuffer[i]);
				}
				assert(
						recycleQueue.enqueue_bulk(rptok,
								std::move_iterator<iter_t>(readBuffer.begin()),
								size));
				if (workQueue.size_approx()) {
					moodycamel::ConsumerToken ctok(workQueue);
					//join in if others are still not finished
					while (workQueue.size_approx()) {
						size_t num = workQueue.try_dequeue_bulk(ctok,
								std::move_iterator<iter_t>(readBuffer.begin()),
								s_bulkSize);
						if (num) {
							for (unsigned i = 0; i < num; ++i) {
								assert(readBuffer[i]);
							}
							assert(
									recycleQueue.enqueue_bulk(rptok,
											std::move_iterator<iter_t>(
													readBuffer.begin()), num));
						}
					}
				}
				good = false;
				kseq_destroy(seq);
				gzclose(fp);
			} else {
				moodycamel::ConsumerToken ctok(workQueue);
				moodycamel::ProducerToken rptok(recycleQueue);
				while (good) {
					if (workQueue.size_approx() >= s_bulkSize) {
						size_t num = workQueue.try_dequeue_bulk(ctok,
								std::move_iterator<iter_t>(readBuffer.begin()),
								s_bulkSize);
						if (num) {
							for (unsigned i = 0; i < num; ++i) {
								//------------------------WORK CODE START---------------------------------------
								assert(readBuffer[i]);
								//------------------------WORK CODE END-----------------------------------------
							}
							assert(
									recycleQueue.enqueue_bulk(rptok,
											std::move_iterator<iter_t>(
													readBuffer.begin()), num));
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
