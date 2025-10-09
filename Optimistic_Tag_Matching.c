// Optimistic Tag Matching with detailed logging to logb.txt
// Fixed missing stdarg.h for variadic logging (va_start / va_end)
#include<unistd.h>  // <---- FIX: needed for usleep
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <stdbool.h>
#include <errno.h>
#include <stdarg.h>   // <---- FIX: needed for va_start / va_end


#define MPI_ANY_SOURCE -1
#define MPI_ANY_TAG -1
#define HASH_TABLE_SIZE 256
#define MAX_EVENTS 200000

typedef struct Message {
    int source_rank;
    int tag;
    void* data;
    size_t size;
    int id;
    pthread_t thread_id;
} Message;

typedef struct ReceiveRequest {
    int source_rank;
    int tag;
    void* buffer;
    size_t size;
    struct ReceiveRequest* next;
    int id;
    bool matched;
    pthread_mutex_t lock;
} ReceiveRequest;

typedef struct HashEntry {
    ReceiveRequest* head;
    pthread_rwlock_t lock;
} HashEntry;

typedef struct OptimisticStructures {
    HashEntry no_wildcard_table[HASH_TABLE_SIZE];
    HashEntry source_wildcard_table[HASH_TABLE_SIZE];
    HashEntry tag_wildcard_table[HASH_TABLE_SIZE];
    ReceiveRequest* both_wildcard_list;
    pthread_rwlock_t both_wildcard_lock;
} OptimisticStructures;

OptimisticStructures opt_struct;

pthread_mutex_t global_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;
FILE* log_fp = NULL;

int matches_successful = 0;
int conflicts_encountered = 0;  // trylock busy on matching recv
int conflicts_resolved = 0;     // locked, but already matched
int messages_processed = 0;
double total_match_time = 0.0;

int next_msg_id = 1;
int next_recv_id = 1;

typedef struct Event {
    double timestamp;
    char type; // 'R' or 'M'
    int source;
    int tag;
    int dest;
} Event;

int compare_events(const void* a, const void* b) {
    double diff = ((Event*)a)->timestamp - ((Event*)b)->timestamp;
    if (diff < 0.0) return -1;
    if (diff > 0.0) return 1;
    return 0;
}

unsigned int hash_function(int source, int tag) {
    return (unsigned int)(((source * 31) + tag) % HASH_TABLE_SIZE);
}

void log_printf(const char* fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    pthread_mutex_lock(&log_lock);
    if (log_fp) {
        vfprintf(log_fp, fmt, ap);
        fprintf(log_fp, "\n");
        fflush(log_fp);
    }
    pthread_mutex_unlock(&log_lock);
    va_end(ap);
}

void init_optimistic_structures() {
    for (int i = 0; i < HASH_TABLE_SIZE; i++) {
        opt_struct.no_wildcard_table[i].head = NULL;
        pthread_rwlock_init(&opt_struct.no_wildcard_table[i].lock, NULL);

        opt_struct.source_wildcard_table[i].head = NULL;
        pthread_rwlock_init(&opt_struct.source_wildcard_table[i].lock, NULL);

        opt_struct.tag_wildcard_table[i].head = NULL;
        pthread_rwlock_init(&opt_struct.tag_wildcard_table[i].lock, NULL);
    }
    opt_struct.both_wildcard_list = NULL;
    pthread_rwlock_init(&opt_struct.both_wildcard_lock, NULL);

    // Open (truncate) log file
    pthread_mutex_lock(&log_lock);
    if (log_fp) fclose(log_fp);
    log_fp = fopen("logb.txt", "w");
    if (!log_fp) {
        perror("Failed to open logb.txt");
        // Still continue but log_fp will be NULL and log_printf will be a no-op
    } else {
        fprintf(log_fp, "=== Optimistic Matching Log ===\n");
        fflush(log_fp);
    }
    pthread_mutex_unlock(&log_lock);
}

bool matches(ReceiveRequest* recv, Message* msg) {
    bool source_match = (recv->source_rank == MPI_ANY_SOURCE ||
                         recv->source_rank == msg->source_rank);
    bool tag_match = (recv->tag == MPI_ANY_TAG ||
                      recv->tag == msg->tag);
    return source_match && tag_match;
}

void insert_receive(ReceiveRequest* recv) {
    if (recv->source_rank == MPI_ANY_SOURCE && recv->tag == MPI_ANY_TAG) {
        pthread_rwlock_wrlock(&opt_struct.both_wildcard_lock);
        recv->next = opt_struct.both_wildcard_list;
        opt_struct.both_wildcard_list = recv;
        pthread_rwlock_unlock(&opt_struct.both_wildcard_lock);
    } else if (recv->source_rank == MPI_ANY_SOURCE) {
        unsigned int hash = (unsigned int)(recv->tag % HASH_TABLE_SIZE);
        pthread_rwlock_wrlock(&opt_struct.tag_wildcard_table[hash].lock);
        recv->next = opt_struct.tag_wildcard_table[hash].head;
        opt_struct.tag_wildcard_table[hash].head = recv;
        pthread_rwlock_unlock(&opt_struct.tag_wildcard_table[hash].lock);
    } else if (recv->tag == MPI_ANY_TAG) {
        unsigned int hash = (unsigned int)(recv->source_rank % HASH_TABLE_SIZE);
        pthread_rwlock_wrlock(&opt_struct.source_wildcard_table[hash].lock);
        recv->next = opt_struct.source_wildcard_table[hash].head;
        opt_struct.source_wildcard_table[hash].head = recv;
        pthread_rwlock_unlock(&opt_struct.source_wildcard_table[hash].lock);
    } else {
        unsigned int hash = hash_function(recv->source_rank, recv->tag);
        pthread_rwlock_wrlock(&opt_struct.no_wildcard_table[hash].lock);
        recv->next = opt_struct.no_wildcard_table[hash].head;
        opt_struct.no_wildcard_table[hash].head = recv;
        pthread_rwlock_unlock(&opt_struct.no_wildcard_table[hash].lock);
    }
}

// Try optimistic match; log steps; update conflict counters.
ReceiveRequest* try_optimistic_match(Message* msg) {
    ReceiveRequest* matched_recv = NULL;

    // 1) exact
    unsigned int exact_hash = hash_function(msg->source_rank, msg->tag);
    pthread_rwlock_rdlock(&opt_struct.no_wildcard_table[exact_hash].lock);
    ReceiveRequest* recv = opt_struct.no_wildcard_table[exact_hash].head;
    while (recv) {
        if (matches(recv, msg)) {
            int tryres = pthread_mutex_trylock(&recv->lock);
            if (tryres == 0) {
                if (!recv->matched) {
                    recv->matched = true;
                    matched_recv = recv;
                    pthread_mutex_unlock(&recv->lock);
                    log_printf("Message(id=%d, src=%d, tag=%d) MATCHED recv(id=%d) in exact-hash bucket[%u]",
                               msg->id, msg->source_rank, msg->tag, recv->id, exact_hash);
                    break;
                } else {
                    conflicts_resolved++;
                    pthread_mutex_unlock(&recv->lock);
                    log_printf("Message(id=%d, src=%d, tag=%d) found recv(id=%d) in exact-hash but already matched",
                               msg->id, msg->source_rank, msg->tag, recv->id);
                }
            } else {
                conflicts_encountered++;
                log_printf("Message(id=%d, src=%d, tag=%d) tried recv(id=%d) in exact-hash but trylock busy (ret=%d)",
                           msg->id, msg->source_rank, msg->tag, recv->id, tryres);
            }
        }
        recv = recv->next;
    }
    pthread_rwlock_unlock(&opt_struct.no_wildcard_table[exact_hash].lock);
    if (matched_recv) return matched_recv;

    // 2) tag-wildcard (source specific, tag wildcard)
    unsigned int tag_hash = (unsigned int)(msg->tag % HASH_TABLE_SIZE);
    pthread_rwlock_rdlock(&opt_struct.tag_wildcard_table[tag_hash].lock);
    recv = opt_struct.tag_wildcard_table[tag_hash].head;
    while (recv) {
        if (matches(recv, msg)) {
            int tryres = pthread_mutex_trylock(&recv->lock);
            if (tryres == 0) {
                if (!recv->matched) {
                    recv->matched = true;
                    matched_recv = recv;
                    pthread_mutex_unlock(&recv->lock);
                    log_printf("Message(id=%d, src=%d, tag=%d) MATCHED recv(id=%d) in tag-wildcard bucket[%u]",
                               msg->id, msg->source_rank, msg->tag, recv->id, tag_hash);
                    break;
                } else {
                    conflicts_resolved++;
                    pthread_mutex_unlock(&recv->lock);
                    log_printf("Message(id=%d, src=%d, tag=%d) found recv(id=%d) in tag-wildcard but already matched",
                               msg->id, msg->source_rank, msg->tag, recv->id);
                }
            } else {
                conflicts_encountered++;
                log_printf("Message(id=%d, src=%d, tag=%d) tried recv(id=%d) in tag-wildcard but trylock busy (ret=%d)",
                           msg->id, msg->source_rank, msg->tag, recv->id, tryres);
            }
        }
        recv = recv->next;
    }
    pthread_rwlock_unlock(&opt_struct.tag_wildcard_table[tag_hash].lock);
    if (matched_recv) return matched_recv;

    // 3) source-wildcard (tag specific)
    unsigned int source_hash = (unsigned int)(msg->source_rank % HASH_TABLE_SIZE);
    pthread_rwlock_rdlock(&opt_struct.source_wildcard_table[source_hash].lock);
    recv = opt_struct.source_wildcard_table[source_hash].head;
    while (recv) {
        if (matches(recv, msg)) {
            int tryres = pthread_mutex_trylock(&recv->lock);
            if (tryres == 0) {
                if (!recv->matched) {
                    recv->matched = true;
                    matched_recv = recv;
                    pthread_mutex_unlock(&recv->lock);
                    log_printf("Message(id=%d, src=%d, tag=%d) MATCHED recv(id=%d) in source-wildcard bucket[%u]",
                               msg->id, msg->source_rank, msg->tag, recv->id, source_hash);
                    break;
                } else {
                    conflicts_resolved++;
                    pthread_mutex_unlock(&recv->lock);
                    log_printf("Message(id=%d, src=%d, tag=%d) found recv(id=%d) in source-wildcard but already matched",
                               msg->id, msg->source_rank, msg->tag, recv->id);
                }
            } else {
                conflicts_encountered++;
                log_printf("Message(id=%d, src=%d, tag=%d) tried recv(id=%d) in source-wildcard but trylock busy (ret=%d)",
                           msg->id, msg->source_rank, msg->tag, recv->id, tryres);
            }
        }
        recv = recv->next;
    }
    pthread_rwlock_unlock(&opt_struct.source_wildcard_table[source_hash].lock);
    if (matched_recv) return matched_recv;

    // 4) both wildcard list
    pthread_rwlock_rdlock(&opt_struct.both_wildcard_lock);
    recv = opt_struct.both_wildcard_list;
    while (recv) {
        // both wildcard always matches (by definition), but we still trylock
        int tryres = pthread_mutex_trylock(&recv->lock);
        if (tryres == 0) {
            if (!recv->matched) {
                recv->matched = true;
                matched_recv = recv;
                pthread_mutex_unlock(&recv->lock);
                log_printf("Message(id=%d, src=%d, tag=%d) MATCHED recv(id=%d) in both-wildcard list",
                           msg->id, msg->source_rank, msg->tag, recv->id);
                break;
            } else {
                conflicts_resolved++;
                pthread_mutex_unlock(&recv->lock);
                log_printf("Message(id=%d, src=%d, tag=%d) found recv(id=%d) in both-wildcard but already matched",
                           msg->id, msg->source_rank, msg->tag, recv->id);
            }
        } else {
            conflicts_encountered++;
            log_printf("Message(id=%d, src=%d, tag=%d) tried recv(id=%d) in both-wildcard but trylock busy (ret=%d)",
                       msg->id, msg->source_rank, msg->tag, recv->id, tryres);
        }
        recv = recv->next;
    }
    pthread_rwlock_unlock(&opt_struct.both_wildcard_lock);

    return matched_recv;
}

typedef struct ThreadData {
    Message* msg;
} ThreadData;

void* process_message_thread(void* arg) {
    ThreadData* data = (ThreadData*)arg;
    Message* msg = data->msg;

    clock_t start = clock();
    ReceiveRequest* matched = try_optimistic_match(msg);
    if (matched) {
        if (matched->buffer && msg->data) {
            memcpy(matched->buffer,
                   msg->data,
                   (msg->size < matched->size) ? msg->size : matched->size);
        }
        pthread_mutex_lock(&global_lock);
        matches_successful++;
        pthread_mutex_unlock(&global_lock);
    }
    clock_t end = clock();

    pthread_mutex_lock(&global_lock);
    messages_processed++;
    total_match_time += ((double)(end - start)) / CLOCKS_PER_SEC;
    pthread_mutex_unlock(&global_lock);

    if (msg->data) free(msg->data);
    free(msg);
    free(data);
    return NULL;
}

void message_arrives_optimistic_sync(int source, int tag, void* data, size_t size) {
    Message* msg = (Message*)malloc(sizeof(Message));
    msg->source_rank = source;
    msg->tag = tag;
    msg->data = malloc(size);
    if (data && size) memcpy(msg->data, data, size);
    msg->size = size;

    pthread_mutex_lock(&global_lock);
    msg->id = next_msg_id++;
    pthread_mutex_unlock(&global_lock);

    ThreadData* td = (ThreadData*)malloc(sizeof(ThreadData));
    td->msg = msg;

    pthread_t t;
    pthread_create(&t, NULL, process_message_thread, td);
    pthread_join(t, NULL);
}

pthread_t message_arrives_optimistic_async(int source, int tag, void* data, size_t size) {
    Message* msg = (Message*)malloc(sizeof(Message));
    msg->source_rank = source;
    msg->tag = tag;
    msg->data = malloc(size);
    if (data && size) memcpy(msg->data, data, size);
    msg->size = size;

    pthread_mutex_lock(&global_lock);
    msg->id = next_msg_id++;
    pthread_mutex_unlock(&global_lock);

    ThreadData* td = (ThreadData*)malloc(sizeof(ThreadData));
    td->msg = msg;

    pthread_t t;
    pthread_create(&t, NULL, process_message_thread, td);
    return t;
}

void post_receive_optimistic(int source, int tag, void* buffer, size_t size) {
    ReceiveRequest* recv = (ReceiveRequest*)malloc(sizeof(ReceiveRequest));
    recv->source_rank = source;
    recv->tag = tag;
    recv->buffer = buffer;
    recv->size = size;
    recv->next = NULL;
    recv->matched = false;
    pthread_mutex_init(&recv->lock, NULL);

    pthread_mutex_lock(&global_lock);
    recv->id = next_recv_id++;
    pthread_mutex_unlock(&global_lock);

    insert_receive(recv);
    log_printf("Posted recv(id=%d, src=%d, tag=%d) inserted", recv->id, recv->source_rank, recv->tag);
}

void simulate_from_trace(const char* recv_file, const char* msg_file, bool conflict_mode) {
    printf("\n=== SIMULATING FROM TRACE FILES (%s) ===\n", conflict_mode ? "WITH CONFLICT" : "NO CONFLICT");

    // reset stats
    matches_successful = 0;
    conflicts_encountered = 0;
    conflicts_resolved = 0;
    messages_processed = 0;
    total_match_time = 0.0;
    next_msg_id = 1;
    next_recv_id = 1;

    init_optimistic_structures();

    Event* events = malloc(sizeof(Event) * MAX_EVENTS);
    int count = 0;
    char line[512];

    FILE* f1 = fopen(recv_file, "r");
    if (!f1) { perror("Error opening recv file"); exit(1); }
    while (fgets(line, sizeof(line), f1)) {
        int rank=0, tag=0, source=0;
        double ts=0.0;
        if (sscanf(line, "%d %d %d %lf", &rank, &tag, &source, &ts) == 4) {
            if (count < MAX_EVENTS) {
                events[count].timestamp = ts;
                events[count].type = 'R';
                events[count].source = source;
                events[count].tag = tag;
                events[count].dest = rank;
                count++;
            }
        }
    }
    fclose(f1);

    FILE* f2 = fopen(msg_file, "r");
    if (!f2) { perror("Error opening msg file"); exit(1); }
    while (fgets(line, sizeof(line), f2)) {
        int rank=0, tag=0, source=0, dest=0;
        double ts=0.0;
        if (sscanf(line, "%d %d %d %d %lf", &rank, &tag, &source, &dest, &ts) == 5) {
            if (count < MAX_EVENTS) {
                events[count].timestamp = ts;
                events[count].type = 'M';
                events[count].source = source;
                events[count].tag = tag;
                events[count].dest = dest;
                count++;
            }
        }
    }
    fclose(f2);

    qsort(events, count, sizeof(Event), compare_events);

    pthread_t* created_threads = NULL;
    int created_count = 0;

    for (int i = 0; i < count; i++) {
        if (events[i].type == 'R') {
            void* buf = malloc(128);
            post_receive_optimistic(events[i].source, events[i].tag, buf, 128);
        } else {
            if (conflict_mode) {
                pthread_t t = message_arrives_optimistic_async(events[i].source, events[i].tag, NULL, 100);
                created_threads = realloc(created_threads, sizeof(pthread_t) * (created_count + 1));
                created_threads[created_count++] = t;
            } else {
                message_arrives_optimistic_sync(events[i].source, events[i].tag, NULL, 100);
            }
        }
    }

    if (conflict_mode && created_count > 0) {
        for (int i = 0; i < created_count; i++) pthread_join(created_threads[i], NULL);
    }
    free(created_threads);

    printf("\n--- Trace Results (%s) ---\n", conflict_mode ? "WITH CONFLICT" : "NO CONFLICT");
    printf("Total events processed from trace: %d\n", count);
    printf("Messages processed: %d\n", messages_processed);
    printf("Successful matches: %d\n", matches_successful);
    printf("Conflicts encountered (trylock busy): %d\n", conflicts_encountered);
    printf("Conflicts resolved (locked but already matched): %d\n", conflicts_resolved);
    printf("Message matching rate: %.2f matches/second\n",
           total_match_time > 0.0 ? matches_successful / total_match_time : 0.0);
    printf("Average match time: %.6f seconds\n",
           messages_processed ? total_match_time / messages_processed : 0.0);

    log_printf("=== SUMMARY (%s) ===", conflict_mode ? "WITH CONFLICT" : "NO CONFLICT");
    log_printf("Total events: %d", count);
    log_printf("Messages processed: %d", messages_processed);
    log_printf("Successful matches: %d", matches_successful);
    log_printf("Conflicts encountered (trylock busy): %d", conflicts_encountered);
    log_printf("Conflicts resolved (locked but already matched): %d", conflicts_resolved);
    log_printf("Message matching rate: %.2f matches/second", total_match_time > 0.0 ? matches_successful / total_match_time : 0.0);
    log_printf("Average match time: %.6f seconds", messages_processed ? total_match_time / messages_processed : 0.0);

    free(events);
}

int main() {
    srand((unsigned)time(NULL));
    // Run No Conflict (synchronous processing)
    simulate_from_trace("recvs_rank_0.txt", "incoming_to_rank_0.txt", false);
    // Run With Conflict (concurrent processing)
    simulate_from_trace("recvs_rank_0.txt", "incoming_to_rank_0.txt", true);

    pthread_mutex_lock(&log_lock);
    if (log_fp) {
        fclose(log_fp);
        log_fp = NULL;
    }
    pthread_mutex_unlock(&log_lock);

    printf("\nLogs written to logb.txt\n");
    return 0;
}
