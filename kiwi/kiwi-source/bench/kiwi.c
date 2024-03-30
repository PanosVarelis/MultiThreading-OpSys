#include <string.h>
#include "../engine/db.h"
#include "../engine/variant.h"
#include "bench.h"
#include <pthread.h>

#define DATAS ("testdb")

struct thread_arg {
		long int start;
		long int end;
		DB* db;
		Variant sk;
		Variant sv;
		char *key;
		int *found;
		int r;
	};

void _write_test(long int count, int r)
{
	int i;
	double cost;
	long long start,end;
	Variant sk, sv;
	DB* db;

	char key[KSIZE + 1];
	char val[VSIZE + 1];
	char sbuf[1024];

	memset(key, 0, KSIZE + 1);
	memset(val, 0, VSIZE + 1);
	memset(sbuf, 0, 1024);

	db = db_open(DATAS);

	start = get_ustime_sec();
	for (i = 0; i < count; i++) {
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "%d adding %s\n", i, key);
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
	cost = end -start;

	printf(LINE);
	printf("|Random-Write	(done:%ld): %.6f sec/op; %.1f writes/sec(estimated); cost:%.3f(sec);\n"
		,count, (double)(cost / count)
		,(double)(count / cost)
		,cost);	
}

void _multi_read_test(struct thread_arg* args)
{
	Variant sk;
	Variant sv;

	DB* db = args->db;
	char *key = args->key;
	int start = args->start;
	int end = args->end;
	int *found = args->found;
	int r = args->r;

	pthread_t tid = pthread_self();
	
	int ret;
	int i;

	for (i = start; i < end; i++) {
		memset(key, 0, KSIZE + 1);

		/* if you want to test random write, use the following */
		if (r)
			_random_key(key, KSIZE);
		else
			snprintf(key, KSIZE, "key-%d", i);
		fprintf(stderr, "Thread %ld: %d searching %s\n", tid, i, key);
		sk.length = KSIZE;
		sk.mem = key;
		ret = db_get(db, &sk, &sv);
		if (ret) {
			//db_free_data(sv.mem);
			*found++;
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
	printf("start: %d, end: %d", start, end);
}


void _read_test(long int count, int r, int t_num)
{
	int found = 0;
	double cost;
	long long start,end;
	char key[KSIZE + 1];
	DB* db;

	db = db_open(DATAS);
	start = get_ustime_sec();
	
	pthread_t id[t_num];
	int t;

	struct thread_arg thread_args[t_num];

	for(t = 0; t < t_num; t++) {
		thread_args[t].key = key;
		thread_args[t].found = &found;
		thread_args[t].db = db;
		thread_args[t].r = r;
		thread_args[t].start = t * (count / t_num);
		if (t == t_num - 1)
			thread_args[t].end = count;
		else
			thread_args[t].end = (t + 1) * (count / t_num);
		if (pthread_create(&id[t], NULL, (void*)_multi_read_test, &thread_args[t]) != 0)
			perror("Failed to create thread");
	}
	for(t = 0; t < t_num; t++) {
		if (pthread_join(id[t], NULL) != 0)
			perror("Failed to join threads");
	}
	
	db_close(db);

	end = get_ustime_sec();
	cost = end - start;
	printf(LINE);
	printf("|Random-Read	(done:%ld, found:%d): %.6f sec/op; %.1f reads /sec(estimated); cost:%.3f(sec)\n",
		count, found,
		(double)(cost / count),
		(double)(count / cost),
		cost);
}

