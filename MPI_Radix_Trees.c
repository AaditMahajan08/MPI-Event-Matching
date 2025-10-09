// CPU-Based MPI Message Matching with Patricia (radix) tries
// Trace-only mode: replays events from trace files (recvs, incoming)
// Build: gcc -std=c11 -O2 -pthread -o cpu_radix_matching_trace_only cpu_radix_matching_trace_only.c

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <ctype.h>
#include <stdarg.h>

#define MPI_ANY_SOURCE -1
#define MPI_ANY_TAG -1

// -----------------------------
// Basic structures (Message/Receive/Queues)
// -----------------------------

typedef struct Message {
    int source_rank;
    int tag;
    void* data;
    size_t size;
    struct Message* next;
    int id;
} Message;

typedef struct ReceiveRequest {
    int source_rank; // expected source or any
    int tag;         // expected tag or any
    void* buffer;
    size_t size;
    struct ReceiveRequest* next; // used in leaf FIFO or both-wild list
    int id;
} ReceiveRequest;

typedef struct Queue {
    Message* msg_head;
    Message* msg_tail;
    int size; // number of messages in UMQ
} Queue;

// -----------------------------
// Patricia (Radix) trie node & leaf queue
// -----------------------------

typedef struct LeafQueue {
    ReceiveRequest* head;
    ReceiveRequest* tail;
    int count;
} LeafQueue;

typedef struct PNode {
    bool is_leaf;
    int bit; // index of bit used for branching (0..63), valid only for internal nodes
    uint64_t key; // valid when is_leaf == true (the stored key)
    struct PNode* left;
    struct PNode* right;
    LeafQueue* lq; // valid for leaves
} PNode;

// -----------------------------
// Globals
// -----------------------------

Queue UMQ;

PNode* exact_root = NULL;   // key: ((uint64_t)tag << 32) | (uint32_t)source
PNode* tag_root = NULL;     // key: (uint64_t)tag
PNode* source_root = NULL;  // key: (uint64_t)source

ReceiveRequest* both_wild_head = NULL;
ReceiveRequest* both_wild_tail = NULL;
int both_wild_count = 0;

pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t log_mutex = PTHREAD_MUTEX_INITIALIZER;

int matches_successful = 0;
double total_match_time = 0.0;
int operations_count = 0;

int next_msg_id = 0;
int next_recv_id = 0;
int prq_count = 0; // number of posted receives across tries + both-wild

FILE* log_fp = NULL; // global log file pointer

// -----------------------------
// Logging helper
// -----------------------------
static void log_event(const char* fmt, ...) {
    pthread_mutex_lock(&log_mutex);
    if (!log_fp) {
        // try to open if not opened yet
        log_fp = fopen("logc.txt", "a");
        if (!log_fp) {
            // as fallback, write to stderr
            va_list args;
            va_start(args, fmt);
            vfprintf(stderr, fmt, args);
            fprintf(stderr, "\n");
            va_end(args);
            pthread_mutex_unlock(&log_mutex);
            return;
        }
    }
    va_list args;
    va_start(args, fmt);
    vfprintf(log_fp, fmt, args);
    fprintf(log_fp, "\n");
    fflush(log_fp);
    va_end(args);
    pthread_mutex_unlock(&log_mutex);
}

// -----------------------------
// Helper utilities
// -----------------------------

static inline int get_bit_u64(uint64_t key, int bit) {
    return (int)((key >> bit) & 1ULL);
}

static inline int highest_diff_bit_u64(uint64_t a, uint64_t b) {
    uint64_t x = a ^ b;
    if (x == 0) return -1;
    return 63 - __builtin_clzll(x);
}

void init_queue(Queue* q) {
    q->msg_head = q->msg_tail = NULL;
    q->size = 0;
}

void enqueue_umq(Message* m) {
    m->next = NULL;
    if (UMQ.msg_tail == NULL) {
        UMQ.msg_head = UMQ.msg_tail = m;
    } else {
        UMQ.msg_tail->next = m;
        UMQ.msg_tail = m;
    }
    UMQ.size++;
}

Message* dequeue_umq(Message* m) {
    Message* curr = UMQ.msg_head;
    Message* prev = NULL;
    while (curr) {
        if (curr == m) {
            if (prev == NULL) {
                UMQ.msg_head = curr->next;
                if (UMQ.msg_head == NULL) UMQ.msg_tail = NULL;
            } else {
                prev->next = curr->next;
                if (curr == UMQ.msg_tail) UMQ.msg_tail = prev;
            }
            UMQ.size--;
            curr->next = NULL;
            return curr;
        }
        prev = curr;
        curr = curr->next;
    }
    return NULL;
}

Message* make_message(int source, int tag, const void* data, size_t size) {
    Message* m = malloc(sizeof(Message));
    m->source_rank = source;
    m->tag = tag;
    m->data = NULL;
    m->size = size;
    m->id = next_msg_id++;
    m->next = NULL;
    if (size > 0 && data) {
        m->data = malloc(size);
        memcpy(m->data, data, size);
    } else if (size > 0) {
        m->data = calloc(1, size);
    }
    return m;
}

ReceiveRequest* make_recv(int source, int tag, void* buffer, size_t size) {
    ReceiveRequest* r = malloc(sizeof(ReceiveRequest));
    r->source_rank = source;
    r->tag = tag;
    r->buffer = buffer;
    r->size = size;
    r->id = next_recv_id++;
    r->next = NULL;
    return r;
}

// LeafQueue ops
LeafQueue* leaf_create() {
    LeafQueue* lq = malloc(sizeof(LeafQueue));
    lq->head = lq->tail = NULL;
    lq->count = 0;
    return lq;
}
void leaf_free(LeafQueue* lq) {
    ReceiveRequest* r = lq->head;
    while (r) {
        ReceiveRequest* n = r->next;
        free(r);
        r = n;
    }
    free(lq);
}
void leaf_append(LeafQueue* lq, ReceiveRequest* r) {
    r->next = NULL;
    if (!lq->head) lq->head = lq->tail = r;
    else { lq->tail->next = r; lq->tail = r; }
    lq->count++;
}
ReceiveRequest* leaf_pop(LeafQueue* lq) {
    ReceiveRequest* r = lq->head;
    if (!r) return NULL;
    lq->head = r->next;
    if (!lq->head) lq->tail = NULL;
    r->next = NULL;
    lq->count--;
    return r;
}

// PNode helpers
PNode* make_leaf_node(uint64_t key) {
    PNode* n = calloc(1, sizeof(PNode));
    n->is_leaf = true;
    n->key = key;
    n->left = n->right = NULL;
    n->lq = leaf_create();
    return n;
}
PNode* make_internal_node(int bit, PNode* left, PNode* right) {
    PNode* n = calloc(1, sizeof(PNode));
    n->is_leaf = false;
    n->bit = bit;
    n->left = left;
    n->right = right;
    n->lq = NULL;
    return n;
}

PNode* rt_find_leaf(PNode* root, uint64_t key) {
    if (!root) return NULL;
    PNode* p = root;
    while (!p->is_leaf) {
        if (get_bit_u64(key, p->bit)) p = p->right;
        else p = p->left;
    }
    return p;
}

void rt_insert_receive(PNode** rootp, uint64_t key, ReceiveRequest* r) {
    if (!(*rootp)) {
        PNode* leaf = make_leaf_node(key);
        leaf_append(leaf->lq, r);
        *rootp = leaf;
        prq_count++;
        return;
    }
    PNode* root = *rootp;
    PNode* p = root;
    PNode* parent = NULL;
    while (!p->is_leaf) {
        parent = p;
        if (get_bit_u64(key, p->bit)) p = p->right;
        else p = p->left;
    }
    if (p->key == key) {
        leaf_append(p->lq, r);
        prq_count++;
        return;
    }
    int diff = highest_diff_bit_u64(key, p->key);
    if (diff < 0) {
        leaf_append(p->lq, r);
        prq_count++;
        return;
    }
    PNode* curr = root;
    PNode* prev = NULL;
    while (!curr->is_leaf && curr->bit > diff) {
        prev = curr;
        if (get_bit_u64(key, curr->bit)) curr = curr->right;
        else curr = curr->left;
    }
    PNode* newleaf = make_leaf_node(key);
    leaf_append(newleaf->lq, r);
    PNode* newnode;
    if (get_bit_u64(key, diff)) newnode = make_internal_node(diff, curr, newleaf);
    else newnode = make_internal_node(diff, newleaf, curr);
    if (prev == NULL) {
        *rootp = newnode;
    } else {
        if (get_bit_u64(key, prev->bit)) prev->right = newnode;
        else prev->left = newnode;
    }
    prq_count++;
}

ReceiveRequest* rt_pop_receive_for_key(PNode* root, uint64_t key) {
    if (!root) return NULL;
    PNode* leaf = rt_find_leaf(root, key);
    if (!leaf || !leaf->is_leaf) return NULL;
    if (leaf->key != key) return NULL;
    ReceiveRequest* r = leaf_pop(leaf->lq);
    if (r) prq_count--;
    return r;
}

bool rt_has_receive_for_key(PNode* root, uint64_t key) {
    if (!root) return false;
    PNode* leaf = rt_find_leaf(root, key);
    if (!leaf || !leaf->is_leaf) return false;
    if (leaf->key != key) return false;
    return (leaf->lq->count > 0);
}

void rt_free(PNode* node) {
    if (!node) return;
    if (node->is_leaf) {
        if (node->lq) {
            leaf_free(node->lq);
            node->lq = NULL;
        }
        free(node);
    } else {
        rt_free(node->left);
        rt_free(node->right);
        free(node);
    }
}

// -----------------------------
// Matching logic using radix trees
// -----------------------------

bool matches_req_msg(ReceiveRequest* recv, Message* msg) {
    bool source_match = (recv->source_rank == MPI_ANY_SOURCE ||
                         recv->source_rank == msg->source_rank);
    bool tag_match = (recv->tag == MPI_ANY_TAG ||
                      recv->tag == msg->tag);
    return source_match && tag_match;
}

void post_receive(int source, int tag, void* buffer, size_t size) {
    clock_t start = clock();
    operations_count++;

    pthread_mutex_lock(&queue_lock);

    Message* m = UMQ.msg_head;
    Message* matched_msg = NULL;
    while (m != NULL) {
        ReceiveRequest temp = { source, tag, buffer, size, NULL, 0 };
        if (matches_req_msg(&temp, m)) {
            matched_msg = m;
            break;
        }
        m = m->next;
    }

    if (matched_msg) {
        Message* removed = dequeue_umq(matched_msg);
        if (removed) {
            if (buffer && removed->data) {
                memcpy(buffer, removed->data, removed->size < size ? removed->size : size);
            }
            if (removed->data) free(removed->data);
            log_event("Message from Rank %d with Tag %d delivered to posted receive", removed->source_rank, removed->tag);
            free(removed);
        }
        matches_successful++;
    } else {
        ReceiveRequest* r = make_recv(source, tag, buffer, size);
        if (source == MPI_ANY_SOURCE && tag == MPI_ANY_TAG) {
            r->next = NULL;
            if (!both_wild_head) both_wild_head = both_wild_tail = r;
            else { both_wild_tail->next = r; both_wild_tail = r; }
            both_wild_count++;
            prq_count++;
            log_event("Posted receive (both-wildcards) id=%d", r->id);
        } else if (source == MPI_ANY_SOURCE) {
            uint64_t key = (uint64_t)(uint32_t)tag;
            rt_insert_receive(&tag_root, key, r);
            log_event("Posted receive (tag-only) tag=%d id=%d", tag, r->id);
        } else if (tag == MPI_ANY_TAG) {
            uint64_t key = (uint64_t)(uint32_t)source;
            rt_insert_receive(&source_root, key, r);
            log_event("Posted receive (source-only) source=%d id=%d", source, r->id);
        } else {
            uint64_t key = (((uint64_t)(uint32_t)tag) << 32) | (uint32_t)source;
            rt_insert_receive(&exact_root, key, r);
            log_event("Posted receive (exact) source=%d tag=%d id=%d", source, tag, r->id);
        }
    }

    pthread_mutex_unlock(&queue_lock);

    clock_t end = clock();
    total_match_time += ((double)(end - start)) / CLOCKS_PER_SEC;
}

Message* make_message_no_payload(int source, int tag) {
    return make_message(source, tag, NULL, 0);
}

void message_arrives(int source, int tag, void* data, size_t size) {
    clock_t start = clock();
    operations_count++;

    pthread_mutex_lock(&queue_lock);

    bool matched = false;

    uint64_t exact_key = (((uint64_t)(uint32_t)tag) << 32) | (uint32_t)source;
    ReceiveRequest* r = rt_pop_receive_for_key(exact_root, exact_key);
    if (r) {
        if (r->buffer && data) memcpy(r->buffer, data, size < r->size ? size : r->size);
        log_event("Message from Rank %d with Tag %d matched in EXACT trie (recv id=%d)", source, tag, r->id);
        free(r);
        matches_successful++;
        matched = true;
        goto done_matching;
    }

    uint64_t tag_key = (uint64_t)(uint32_t)tag;
    r = rt_pop_receive_for_key(tag_root, tag_key);
    if (r) {
        if (r->buffer && data) memcpy(r->buffer, data, size < r->size ? size : r->size);
        log_event("Message from Rank %d with Tag %d matched in TAG-ONLY trie (recv id=%d)", source, tag, r->id);
        free(r);
        matches_successful++;
        matched = true;
        goto done_matching;
    }

    uint64_t src_key = (uint64_t)(uint32_t)source;
    r = rt_pop_receive_for_key(source_root, src_key);
    if (r) {
        if (r->buffer && data) memcpy(r->buffer, data, size < r->size ? size : r->size);
        log_event("Message from Rank %d with Tag %d matched in SOURCE-ONLY trie (recv id=%d)", source, tag, r->id);
        free(r);
        matches_successful++;
        matched = true;
        goto done_matching;
    }

    if (both_wild_head != NULL) {
        ReceiveRequest* rr = both_wild_head;
        both_wild_head = rr->next;
        if (!both_wild_head) both_wild_tail = NULL;
        both_wild_count--;
        prq_count--;
        if (rr->buffer && data) memcpy(rr->buffer, data, size < rr->size ? size : rr->size);
        log_event("Message from Rank %d with Tag %d matched in BOTH-WILDCARDS list (recv id=%d)", source, tag, rr->id);
        free(rr);
        matches_successful++;
        matched = true;
        goto done_matching;
    }

    {
        Message* m = make_message(source, tag, data, size);
        enqueue_umq(m);
        log_event("Message from Rank %d with Tag %d stored in UMQ", source, tag);
    }

done_matching:
    pthread_mutex_unlock(&queue_lock);

    clock_t end = clock();
    total_match_time += ((double)(end - start)) / CLOCKS_PER_SEC;
}

// -----------------------------
// Trace replay (robust version)
// -----------------------------

typedef struct Event {
    double timestamp;
    char type; // 'R' or 'M'
    int source;
    int tag;
    int dest;
    long seq;
} Event;

int compare_events(const void* a, const void* b) {
    const Event ea = (const Event)a;
    const Event eb = (const Event)b;
    if (ea->timestamp < eb->timestamp) return -1;
    if (ea->timestamp > eb->timestamp) return 1;
    if (ea->seq < eb->seq) return -1;
    if (ea->seq > eb->seq) return 1;
    return 0;
}

void free_remaining_queues_and_trees() {
    Message* m = UMQ.msg_head;
    while (m) {
        Message* n = m->next;
        if (m->data) free(m->data);
        free(m);
        m = n;
    }
    UMQ.msg_head = UMQ.msg_tail = NULL;
    UMQ.size = 0;

    while (both_wild_head) {
        ReceiveRequest* n = both_wild_head->next;
        free(both_wild_head);
        both_wild_head = n;
    }
    both_wild_head = both_wild_tail = NULL;
    both_wild_count = 0;
    prq_count = 0;

    rt_free(exact_root); exact_root = NULL;
    rt_free(tag_root); tag_root = NULL;
    rt_free(source_root); source_root = NULL;
}

void simulate_from_trace(const char* recv_file, const char* msg_file) {
    free_remaining_queues_and_trees();
    matches_successful = 0;
    total_match_time = 0.0;
    operations_count = 0;
    next_msg_id = next_recv_id = prq_count = 0;

    // Open (truncate) log file at start of simulation
    pthread_mutex_lock(&log_mutex);
    if (log_fp) { fclose(log_fp); log_fp = NULL; }
    log_fp = fopen("logc.txt", "w");
    if (!log_fp) {
        fprintf(stderr, "Warning: could not open logc.txt for writing. Falling back to stderr.\n");
    }
    pthread_mutex_unlock(&log_mutex);

    Event* events = NULL;
    size_t cap = 0;
    size_t cnt = 0;
    long seq_counter = 0;

    #define APPEND_EVENT(ev) do { \
        if (cnt + 1 > cap) { size_t newcap = cap ? cap * 2 : 4096; Event* tmp = realloc(events, newcap * sizeof(Event)); if (!tmp) { perror("realloc"); free(events); return; } events = tmp; cap = newcap; } \
        events[cnt++] = (ev); \
    } while(0)

    char line[1024];
    FILE* f1 = fopen(recv_file, "r");
    if (!f1) { perror("Error opening recv file"); free(events); return; }
    while (fgets(line, sizeof(line), f1)) {
        char *p = line;
        while (*p && isspace((unsigned char)*p)) p++;
        if (*p == '\0' || *p == '#' || *p == '\n') continue;
        int rank, tag, source;
        double ts;
        if (sscanf(line, "%d %d %d %lf", &rank, &tag, &source, &ts) == 4) {
            Event e = {ts, 'R', source, tag, rank, seq_counter++};
            APPEND_EVENT(e);
        } else if (sscanf(line, "%d %d %lf", &tag, &source, &ts) == 3) {
            Event e = {ts, 'R', source, tag, -1, seq_counter++};
            APPEND_EVENT(e);
        } else { continue; }
    }
    fclose(f1);

    FILE* f2 = fopen(msg_file, "r");
    if (!f2) { perror("Error opening msg file"); free(events); return; }
    while (fgets(line, sizeof(line), f2)) {
        char *p = line;
        while (*p && isspace((unsigned char)*p)) p++;
        if (*p == '\0' || *p == '#' || *p == '\n') continue;
        int rank, tag, source, dest;
        double ts;
        if (sscanf(line, "%d %d %d %d %lf", &rank, &tag, &source, &dest, &ts) == 5) {
            Event e = {ts, 'M', source, tag, dest, seq_counter++};
            APPEND_EVENT(e);
        } else if (sscanf(line, "%d %d %d %lf", &tag, &source, &dest, &ts) == 4) {
            Event e = {ts, 'M', source, tag, dest, seq_counter++};
            APPEND_EVENT(e);
        } else { continue; }
    }
    fclose(f2);
    #undef APPEND_EVENT

    if (cnt == 0) {
        log_event("No events loaded from traces.");
        free(events);
        return;
    }

    qsort(events, cnt, sizeof(Event), compare_events);

    for (size_t i = 0; i < cnt; ++i) {
        Event *ev = &events[i];
        if (ev->type == 'R') {
            post_receive(ev->source, ev->tag, NULL, 100);
        } else {
            message_arrives(ev->source, ev->tag, NULL, 100);
        }
    }

    double avg = (operations_count > 0) ? total_match_time / (double)operations_count : 0.0;

    printf("--- Trace Replay Results ---\n");
    printf("Total events: %zu\n", cnt);
    printf("Successful matches: %d\n", matches_successful);
    printf("UMQ size: %d, PRQ count: %d\n", UMQ.size, prq_count);
    printf("Avg match time/op: %.9f sec (operations_count=%d)\n", avg, operations_count);

    free(events);

    // close log file here (it will be reopened on next run if needed)
    pthread_mutex_lock(&log_mutex);
    if (log_fp) { fclose(log_fp); log_fp = NULL; }
    pthread_mutex_unlock(&log_mutex);
}

// -----------------------------
// Main (trace-only mode)
// -----------------------------
int main() {

    init_queue(&UMQ);
    simulate_from_trace("recvs_rank_0.txt", "incoming_to_rank_0.txt");

    return 0;
}
