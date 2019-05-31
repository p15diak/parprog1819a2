// ... ο κώδικάς σας για την υλοποίηση του quicksort
// με pthreads και thread pool...
//gcc -pthread -O2 quicksort.c -o quicksort
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

#define N 1000000     
#define THREADS 4  /* ari8mos threads   */  
#define SIZE 100        
#define THRESHOLD 10    
#define WORK 0 /* xreiazetai epexergasia*/
#define DONE 1/* exei oloklhrw8h h epexergasia*/
#define END 2 /*termatismos meta thn taxinomhsh*/

struct message {
    int type; //typos
    int start; //arxh
    int end; //telos
};

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t msg_in = PTHREAD_COND_INITIALIZER;
pthread_cond_t msg_out = PTHREAD_COND_INITIALIZER;

struct message msg_Queue[N];
int queue_in = 0, queue_out = 0;
int msg_counter = 0;

void msg_send(int type, int start, int end) {// dhmiourgoume msg_send
    pthread_mutex_lock(&mutex);
    while (msg_counter >= N) {
        printf("\nProducer locked\n");
        pthread_cond_wait(&msg_out, &mutex);
    }
    
    msg_Queue[queue_in].type = type;
    msg_Queue[queue_in].start = start;
    msg_Queue[queue_in].end = end;
    queue_in = (queue_in + 1) % N;
    msg_counter=msg_counter+1;

    pthread_cond_signal(&msg_in);
    pthread_mutex_unlock(&mutex);
}

void msg_rcv(int *type, int *start, int *end) { //dhmiourgoume thn msg_rcv 
    pthread_mutex_lock(&mutex);
    while (msg_counter < 1) {
        printf("\nConsumer locked\n");
        pthread_cond_wait(&msg_in, &mutex);
    }

    *type = msg_Queue[queue_out].type;
    *start = msg_Queue[queue_out].start;
    *end = msg_Queue[queue_out].end;
    queue_out = (queue_out + 1) % N;
    msg_counter= msg_counter-1;

    pthread_cond_signal(&msg_out);
    pthread_mutex_unlock(&mutex);
}

void swap(double *x, double *y) {
    double temp = *x;
    *x= *y;
    *y = temp;
} 

int partition(double *array, int n) { //partition sort for the quicksort
    int first = 0;
    int mid = n/2;
    int last = n-1;
    if (array[first] > array[mid]) {
        swap(array+first, array+mid);
    }
    if (array[mid] > array[last]) {
        swap(array+mid, array+last);
    }
    if (array[first] > array[mid]) {
        swap(array+first, array+mid);
    }
    double pivot = array[mid];
    int i, j;
    for (i=1, j=n-2;; i++, j--) {
        while (array[i] < pivot){
		 i++;
	}
        while (array[j] > pivot) {
        	j--;
		}
		
        if (i>=j) break;
        swap(array+i, array+j);
    }
    return i;
}


void isort(double *array, int n) {//insertion sort
	double t;
	int j;
	for (int i=1; i<n; i++) {
		j=i;
		while ((j>0) && (array[j-1] > array[j])) {
			swap(array+(j-1),array+(j));
			j--;
		}
	}
}


void quicksort(double *array, int n, int t) {
	if (n<=THRESHOLD) {	
		isort(array, n);
		return;
	}

	int k;


	if (t !=2 ) {		

		k = partition(array,n);
	//First half
		quicksort(array, k, t);//1rst half sort
		msg_send(0,*array,*array+k);	
	
		quicksort(array+k, n-k, t);//2 half sort
		msg_send(0,*array+k,n);		
	}
}







void *thread_func(void *params) {
    double *array = (double*) params;
    int t, s, e; //s for the start e for the end and t for type
    msg_rcv(&t, &s, &e);
    while (t != END) { 
        if (t == DONE) {
            msg_send(DONE, s, e);
        } else if (t == WORK) {
            if (e-s <= THRESHOLD) {
                isort(array+s, e-s);
                msg_send(DONE, s, e);
            } else {
                int part = partition(array+s, e-s);
                msg_send(WORK, s, s+part);
                msg_send(WORK, s+part, e);
            }
        }
        msg_rcv(&t, &s, &e);
    }
    msg_send(END, 0, 0);
    printf("done!\n");
    pthread_exit(NULL);
}

int main() {
    double *array = (double*) malloc(sizeof(double) * SIZE);
    if (array == NULL) {
        printf("Error \n");
        exit(1);
    }
    for (int i=0; i<SIZE; i++) {
        array[i] = (double) rand()/RAND_MAX;
    }
    pthread_t pool_t[THREADS];
    for (int i=0; i<THREADS; i++){
        if (pthread_create(&pool_t[i], NULL, thread_func, array) != 0) {
            printf("Thread pool creation failed \n");
            free(array);
            exit(1);
        }
    }
    msg_send(WORK, 0, SIZE);
    int t, s, e;
    int count = 0;
    msg_rcv(&t, &s, &e);
    while (1) {
        if (t == DONE) {
            count += e-s;
            printf(" DONE ! %d %d\n", count, SIZE);
            printf("Partition done !: (%d, %d)\n", s, e);
            if (count == SIZE) {
                break;
            }
        } else {
            msg_send(t, s, e);
        }
        msg_rcv(&t, &s, &e);
    }
    msg_send(END, 0, 0);
    for (int i=0; i<THREADS; i++) {
        pthread_join(pool_t[i], NULL);
    }
    int i;
    for (i=0; i<SIZE-1; i++) {
        if (array[i] > array[i+1]) {
            printf("Error! Array failed to sort. a[%d] = %lf, a[%d] = %lf\n", i, array[i], i+1, array[i+1]);
            break;
        }
    }
    pthread_mutex_destroy(&mutex);
    pthread_cond_destroy(&msg_in);
    pthread_cond_destroy(&msg_out);
    free(array);
    return 0;
}

