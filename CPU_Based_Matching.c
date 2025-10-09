// cpu_based_matching.c
// Part A: CPU-Based MPI Message Matching Implementation (Trace-based Input Enabled)
// Logs are written to loga.txt; only final stats printed to terminal.

#include <stdio.h> //for input output and file operations
#include <stdlib.h> //for memory allocation and general utilities
#include <string.h> //for string operations
#include <time.h> //to measure how long an operation takes
#include <pthread.h> //for thread and mutex support
#include <stdbool.h> //enables bool, true and false in C
#include <stdarg.h> //for variadic logging

#define MPI_ANY_SOURCE -1 //standard mpi wildcard value
#define MPI_ANY_TAG -1 //standard mpi wildcard value
#define MAX_QUEUE_SIZE 10000 //max size of queue, prq and umq
#define NUM_TESTS 10000 //number of test events

// Message structure
// Represents a message being sent in mpi
typedef struct Message {
    int source_rank; //sender's ID
    int tag; //user defined label to categorize the message
    void* data; //content of the message
    size_t size; //size of data in bytes
    struct Message* next; ///pointer to the next message in the queue
    int id; // For tracking each message
} Message;

// Receive request structure
// Represents a request being sent in moi
typedef struct ReceiveRequest {
    int source_rank; // Expected source or MPI_ANY_SOURCE
    int tag;        // Expected tag or MPI_ANY_TAG
    void* buffer;   // Buffer to store received data
    size_t size; //size of the buffer
    struct ReceiveRequest* next; //next receive in prq
    int id; // For tracking
} ReceiveRequest;

// Queue structure
typedef struct Queue { //a common structure for both prq and umq
    Message* msg_head; //last message in the queue
    Message* msg_tail; //most recent message in the queue
    ReceiveRequest* recv_head; //""
    ReceiveRequest* recv_tail; //""
    int size; //total items(both messages and receives)
} Queue;

// Event structure for reading traces
typedef struct Event {
    double timestamp; //time of the event
    char type; // 'M' = message, 'R' = receive
    int source;
    int tag;
    int dest;
    //source tag and dest are event specific info
} Event;

// Global queues
Queue PRQ; //queue for prq
Queue UMQ; //queue for umq
//the line below is for the case where multiple threads have been used for implementation to avoid race conditions in case of updations made to umq and prq, operations like enqueue and dequeue involve multiple steps
pthread_mutex_t queue_lock = PTHREAD_MUTEX_INITIALIZER; //pthread_mutex_t declares a mutex object from the posix threads library

// Logging
static FILE* log_file = NULL;
pthread_mutex_t log_lock = PTHREAD_MUTEX_INITIALIZER;

// Statistics
int matches_successful = 0; //variable to keep track of total successful matches
double total_match_time = 0.0; //time for matches
int operations_count = 0; //count of operations

int next_msg_id = 0; //unique ids for messages
int next_recv_id = 0; //unique ids for receives

// ---------- Helper logging function (thread-safe) ----------
void write_log(const char* fmt, ...) {
    if (!log_file) return;
    pthread_mutex_lock(&log_lock);
    va_list args;
    va_start(args, fmt);
    vfprintf(log_file, fmt, args);
    fprintf(log_file, "\n");
    fflush(log_file);
    va_end(args);
    pthread_mutex_unlock(&log_lock);
}

// initialize queue
void init_queue(Queue* q) { //initialise the queue
    q->msg_head = NULL;
    q->msg_tail = NULL;
    q->recv_head = NULL;
    q->recv_tail = NULL;
    q->size = 0;
}

//helper function to match an incoming message with a request in prq
//uses source and tag to match the message with the receive
bool matches(ReceiveRequest* recv, Message* msg) {
    bool source_match = (recv->source_rank == MPI_ANY_SOURCE ||
                         recv->source_rank == msg->source_rank);
    bool tag_match = (recv->tag == MPI_ANY_TAG ||
                      recv->tag == msg->tag);
    return source_match && tag_match;
}

//helper function to enqueue the message
void enqueue_message(Queue* q, Message* msg) {
    msg->next = NULL; //set the next ptr of the message to null
    if (q->msg_tail == NULL) { //if the queue is empty
        q->msg_head = q->msg_tail = msg; //then set both the head and the tail to that message
    } else {
        q->msg_tail->next = msg; //otherwise point the next of the tail to the msg
        q->msg_tail = msg; //now set the tail to the new msg
    }
    q->size++; //increment the size of the queue
}

//same helper function but for the requests
void enqueue_receive(Queue* q, ReceiveRequest* recv) {
    recv->next = NULL;
    if (q->recv_tail == NULL) {
        q->recv_head = q->recv_tail = recv;
    } else {
        q->recv_tail->next = recv;
        q->recv_tail = recv;
    }
    q->size++;
}

//helper function to dequeue messages, returns NULL if the message wasnt found or the pointer to the message
Message* dequeue_message(Queue* q, Message* msg) {
    Message* curr = q->msg_head; //curr is used to traverse the linked list
    Message* prev = NULL; //keeps track of the previous node while traversing
    while (curr) { //loop to traverse the linked list
        if (curr == msg) { //if the message is found
            if (prev == NULL) { //if the the found message is the head of the linked list
                q->msg_head = curr->next; //set the head to the next node
                if (q->msg_head == NULL) q->msg_tail = NULL; //if the queue is empty after the removal of the first element, then set the tail to null as well
            } else { //if the message is not the first node of the queue
                prev->next = curr->next; //set the next of the previous ptr to the next node
                if (curr == q->msg_tail) q->msg_tail = prev; //if the node is the last node, then set the tail to the previous node
            }
            q->size--; //decrement the size of the queue
            curr->next = NULL;
            return curr; //return a pointer to the message dequeued
        }
        //iterate
        prev = curr;
        curr = curr->next;
    }
    return NULL; //return null if the node wasnt found
}

//same for dequeuing a receive
ReceiveRequest* dequeue_receive(Queue* q, ReceiveRequest* recv) {
    ReceiveRequest* curr = q->recv_head;
    ReceiveRequest* prev = NULL;
    while (curr) {
        if (curr == recv) {
            if (prev == NULL) {
                q->recv_head = curr->next;
                if (q->recv_head == NULL) q->recv_tail = NULL;
            } else {
                prev->next = curr->next;
                if (curr == q->recv_tail) q->recv_tail = prev;
            }
            q->size--;
            curr->next = NULL;
            return curr;
        }
        prev = curr;
        curr = curr->next;
    }
    return NULL;
}

//handles the request when it is posted, it first checks the umq and then is appended in the prq
void post_receive(int source, int tag, void* buffer, size_t size) {
    clock_t start = clock(); //start the time
    operations_count++; //posting of the receive request is an operation and thereby increase it

    pthread_mutex_lock(&queue_lock); //enter critical section

    Message* msg = UMQ.msg_head; //a pointer to traverse the unexpected messages queue
    bool found = false; //a variable to know if the messages has been found or not

    while (msg != NULL) { //iterate the umq
        ReceiveRequest temp_recv = {source, tag, buffer, size, NULL, 0}; //initialise an object of the type receive
        if (matches(&temp_recv, msg)) { //check if the msg and the request match
            if (buffer && msg->data) { //if both buffer and receive exist
                memcpy(buffer, msg->data, msg->size < size ? msg->size : size); //copy the message into the buffer, if the size of message is greater than size of the buffer then only copy till size of the buffer
            }

            Message* removed = dequeue_message(&UMQ, msg); //dequeue the message if found
            if (removed) {
                write_log("Message from Rank %d with Tag %d matched with Receive id %d", removed->source_rank, removed->tag, next_recv_id);
                if (removed->data) free(removed->data); //avoid memory leaks
                free(removed);
            }

            matches_successful++; //increment successful match
            found = true; //set the found flag to true
            break;
        }
        msg = msg->next; //iterate
    }

    //if the message hasnt been found in UMQ, enqueue the receive into PRQ
    if (!found) {
        ReceiveRequest* recv = (ReceiveRequest*)malloc(sizeof(ReceiveRequest));
        recv->source_rank = source;
        recv->tag = tag;
        recv->buffer = buffer;
        recv->size = size;
        recv->id = next_recv_id++;
        recv->next = NULL;
        enqueue_receive(&PRQ, recv);
        write_log("Receive expecting Source %d with Tag %d posted in PRQ", source, tag);
    }

    pthread_mutex_unlock(&queue_lock);
    clock_t end = clock();
    total_match_time += ((double)(end - start)) / CLOCKS_PER_SEC;
}

void message_arrives(int source, int tag, void* data, size_t size) {
    clock_t start = clock();
    operations_count++;

    pthread_mutex_lock(&queue_lock);

    ReceiveRequest* recv = PRQ.recv_head;
    bool found = false;

    while (recv != NULL) {
        Message temp_msg = {source, tag, data, size, NULL, 0};
        if (matches(recv, &temp_msg)) {
            if (recv->buffer && data) {
                memcpy(recv->buffer, data, size < recv->size ? size : recv->size);
            }

            ReceiveRequest* removed = dequeue_receive(&PRQ, recv);
            if (removed) {
                write_log("Message from Rank %d with Tag %d matched with Receive id %d", source, tag, removed->id);
                free(removed);
            }
            matches_successful++;
            found = true;
            break;
        }
        recv = recv->next;
    }

    if (!found) {
        Message* msg = (Message*)malloc(sizeof(Message));
        msg->source_rank = source;
        msg->tag = tag;
        msg->data = NULL;
        msg->size = size;
        msg->id = next_msg_id++;
        msg->next = NULL;

        if (size > 0) {
            msg->data = malloc(size);
            if (data) memcpy(msg->data, data, size);
            else memset(msg->data, 0, size);
        }

        enqueue_message(&UMQ, msg);
        write_log("Message from Rank %d with Tag %d stored in UMQ", source, tag);
    }

    pthread_mutex_unlock(&queue_lock);
    clock_t end = clock();
    total_match_time += ((double)(end - start)) / CLOCKS_PER_SEC;
}

// === NEW SECTION START ===
// Read traces and simulate real matching
int compare_events(const void* a, const void* b) {
    double diff = ((Event*)a)->timestamp - ((Event*)b)->timestamp;
    return (diff < 0) ? -1 : (diff > 0);
}

void simulate_from_trace(const char* recv_file, const char* msg_file) {
    // initialize queues and stats
    init_queue(&PRQ);
    init_queue(&UMQ);
    matches_successful = 0;
    total_match_time = 0.0;
    operations_count = 0;
    next_msg_id = 0;
    next_recv_id = 0;

    // open log file (overwrite)
    pthread_mutex_lock(&log_lock);
    if (log_file) fclose(log_file);
    log_file = fopen("loga.txt", "w");
    if (!log_file) {
        perror("Unable to open log file loga.txt");
        pthread_mutex_unlock(&log_lock);
        exit(1);
    }
    pthread_mutex_unlock(&log_lock);

    Event* events = malloc(sizeof(Event) * 200000);
    int count = 0;

    // Read receives
    FILE* f1 = fopen(recv_file, "r");
    if (!f1) { perror("Error opening recv file"); exit(1); }
    char line[256];
    fgets(line, sizeof(line), f1); // skip header
    while (fgets(line, sizeof(line), f1)) {
        int rank, tag, source;
        double ts;
        if (sscanf(line, "%d %d %d %lf", &rank, &tag, &source, &ts) == 4) {
            events[count++] = (Event){ts, 'R', source, tag, rank};
        }
    }
    fclose(f1);

    // Read messages
    FILE* f2 = fopen(msg_file, "r");
    if (!f2) { perror("Error opening msg file"); exit(1); }
    fgets(line, sizeof(line), f2); // skip header
    while (fgets(line, sizeof(line), f2)) {
        int rank, tag, source, dest;
        double ts;
        if (sscanf(line, "%d %d %d %d %lf", &rank, &tag, &source, &dest, &ts) == 5) {
            events[count++] = (Event){ts, 'M', source, tag, dest};
        }
    }
    fclose(f2);

    qsort(events, count, sizeof(Event), compare_events);

    for (int i = 0; i < count; i++) {
        if (events[i].type == 'R')
            post_receive(events[i].source, events[i].tag, NULL, 100);
        else
            message_arrives(events[i].source, events[i].tag, NULL, 100);
    }

    // Final stats printed to terminal only
    printf("\n--- Trace Results ---\n");
    printf("Total events: %d\n", count);
    printf("Successful matches: %d\n", matches_successful);
    printf("Remaining in UMQ: %d, PRQ: %d\n", UMQ.size, PRQ.size);
    printf("Avg match time: %.9f sec\n", (count > 0) ? (total_match_time / (double)count) : 0.0);

    // close log and free resources
    pthread_mutex_lock(&log_lock);
    if (log_file) {
        fflush(log_file);
        fclose(log_file);
        log_file = NULL;
    }
    pthread_mutex_unlock(&log_lock);

    free(events);
}
// === NEW SECTION END ===

int main() {
    // run simulation (no other terminal prints except final stats inside simulate_from_trace)
    simulate_from_trace("recvs_rank_0.txt", "incoming_to_rank_0.txt");

    return 0;
}
