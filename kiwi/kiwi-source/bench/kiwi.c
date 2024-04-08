#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"

#define DATAS ("testdb")

// STRUCT TO PASS ARGS TO EACH THREAD
// USED FOR READ OP
struct thread_arg {
	long int *i;
	long int end;
	DB* db;
	int found;
	int r;
	double cost;
	long long time;
	long int thread_found;
};
// STRUCT TO PASS ARGS TO WRITER
struct write_test_arg {
	long int write_count;
	int r;
	double cost;
};

// WRITE OP
void _write_test(struct write_test_arg* args)
{	
	int i;
	long long start,end;
	Variant sk, sv;
	DB* db;

	long int count = args->write_count;
	int r = args->r;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	pthread_t wr_tid = pthread_self();			// USED TO DIFFERENTIATE THE THREADS IN OUTPUT

	db = db_open(DATAS);

	start = get_ustime_sec();
	
	for (i = 0; i < count; i++) {
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "Thread %ld: %d adding %s\n", wr_tid, i, key);
		snprintf(val, VSIZE, "val-%d", i);

		sk.length = KSIZE;
		sk.mem = key;
		sv.length = VSIZE;
		sv.mem = val;

		db_add(db, &sk, &sv);

		if ((i % 10000) == 0) {
			fprintf(stderr,"random write finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}
	
	db_close(db);

	end = get_ustime_sec();
	args->cost = end - start;				// USED IN OPERATION MANAGER, TO CALC SUM OF COSTS, WRITE OP
}

// READ OP 
void _threaded_read_test(struct thread_arg* args)
{
	Variant sk;
	Variant sv;
	char key[KSIZE + 1];

	DB* db = args->db;
	int end = args->end;
	int r = args->r;

	//pthread_t tid = pthread_self();			// USED TO DIFFERENTIATE THREADS IN OUTPUT
	int ret;
	int i;
	int count = 0;
	long long begin = get_ustime_sec();
	while (*(args->i) < end) {
		i = *(args->i);
		*(args->i) += 1;
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		//fprintf(stderr, "Thread %ld: %d searching %s\n", tid, i, key);
		sk.length = KSIZE;
		sk.mem = key;

		
		ret = db_get(db, &sk, &sv);
		if (ret) {
			//db_free_data(sv.mem);
			count++;
			args->thread_found += 1;
		} else {
			INFO("not found key#%s", 
					sk.mem);
    	}

		if ((i % 10000) == 0) {
			fprintf(stderr,"random read finished %d ops%30s\r", 
					i, 
					"");

			fflush(stderr);
		}
	}
	long long finish = get_ustime_sec();
	args->time = finish - begin;
	args->found += count;		// USED TO CALCULATE SUM OF ITEMS FOUND, IN OPERATION MANAGER, READ OP
}

// CREATE'S NUMBER OF READERS
// AND INITIALISES EACH READER ARGS
double* _read_test(long int count, int r, int t_num)
{
	long int i = 0;
	int found = 0;
	double cost = 0.0;
	long long start,end;
	
	DB* db;
	
	double* result = (double*)malloc(2*sizeof(double));

	db = db_open(DATAS);
	start = get_ustime_sec();
	
	pthread_t id[t_num];
	int t;

	struct thread_arg thread_args[t_num];

	for(t = 0; t < t_num; t++) {						    // INITIALISE ARGUEMENTS 
		thread_args[t].found = 0;							// FOR EACH NEW READER
		thread_args[t].db = db;								//
		thread_args[t].r = r;								//
		thread_args[t].i = &i;			// EACH NEW READER HAS CERTAIN RANGE OF KEYS
		thread_args[t].thread_found = 0;
		if (t == t_num - 1)
			thread_args[t].end = count;
		else
			thread_args[t].end = (t + 1) * (count / t_num);
		if (pthread_create(&id[t], NULL, (void*)_threaded_read_test, &thread_args[t]) != 0)
			perror("Failed to create thread");
	}
	for(t = 0; t < t_num; t++) {
		if (pthread_join(id[t], NULL) != 0)
			perror("Failed to join threads");
	}
	
	db_close(db);

	end = get_ustime_sec();
	for(int t = 0; t < t_num; t++){
		found += thread_args[t].found;
		printf("Thread: %ld, Time: %lld, keys found: %ld\n", id[t], thread_args[t].time, thread_args[t].thread_found);
	}
	cost = end - start;
	result[0] = (double)found;			// USED IN OPERATION MANAGER TO CALCULATE SUM OF KEYS FOUND
	result[1] = cost;					// SAME FOR SUM OF COST, READ OP
	return result;
}

// HANDLES THE READ AND RIGHT OPERATIONS
void _operation_manager(long int write_count, int read_threads, long int read_count, int r){
	pthread_t wr_t;
	__init();
	double* res;
	double cost;

	struct write_test_arg wr_arg;
	wr_arg.write_count = write_count;
	wr_arg.r = r;

	pthread_create(&wr_t, NULL, (void*)_write_test, &wr_arg);
	
	res = _read_test(read_count, r, read_threads);
	
	pthread_join(wr_t, NULL);

	cost = wr_arg.cost;

	// FINAL RESULTS OUTPUT STARTS HERE
	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,write_count, (double)(cost / write_count)
		,(double)(write_count / cost)
		,cost);
	
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		read_count, (int)res[0],
		(double)(res[1] / read_count),
		(double)(read_count / res[1]),
		res[1]);
	
	printf(LINE);
	printf("|Total time elapsed: %.3f(sec)\n", cost+res[1]);
	
	// FREEING UP THREADS AND MEMORY
	__destroy();
	free(res);
}
