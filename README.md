# MPI Event Matching Simulation

This repository contains implementations and analysis of different **MPI message matching strategies** in C/C++:

- **CPU-Based Matching**
- **Optimistic Tag Matching**
- **Radix Trie–Based Matching**

The project demonstrates correctness, efficiency, and performance of these strategies under various workloads.

---

## Table of Contents

- [Problem Statement](#problem-statement)  
- [Motivation](#motivation)  
- [Objectives](#objectives)  
- [Implementation](#implementation)  
- [Compilation and Execution](#compilation-and-execution)  
- [Comparison and Results](#comparison-and-results)  
- [Conclusion](#conclusion)  
- [Acknowledgment](#acknowledgment)  

---

## Problem Statement

MPI (Message Passing Interface) applications rely on efficient message matching to ensure that incoming messages are correctly delivered to posted receive requests.  
Traditional CPU-based matching can become a bottleneck, while parallel and data-structured approaches like optimistic tag matching and radix trie–based matching aim to improve throughput and efficiency.

---

## Motivation

Efficient message matching is critical for high-performance computing and distributed systems. Delays in matching can reduce the performance of scientific simulations, large-scale data processing, and parallel applications.  
This project provides hands-on experience with concurrency, synchronization, and efficient data structures in MPI.

---

## Objectives

1. Implement CPU-based MPI message matching with posted receive and unexpected message queues.  
2. Implement optimistic tag matching using multi-threading and hash-based indexing.  
3. Implement radix trie–based matching for logarithmic lookup efficiency.  
4. Compare correctness, efficiency, and performance of all approaches.  
5. Analyze the impact of message order, conflicts, and queue length on matching rate and runtime.

---

## Implementation

### 1. CPU-Based MPI Message Matching
- Sequential scanning of Posted Receive Queue (PRQ) and Unexpected Message Queue (UMQ).  
- Handles messages sequentially; simple and deterministic.  
- Code file: [`CPU_Based_Matching`](./CPU_Based_Matching)

### 2. Optimistic Tag Matching
- Multi-threaded matching using hash tables and fine-grained locks.  
- Concurrent processing of events with conflict resolution.  
- Code file: [`Optimistic_Tag_Matching`](./Optimistic_Tag_Matching)

### 3. Radix Trie–Based Matching
- Uses Patricia (radix) tries for key-based lookups.  
- Reduces search complexity from linear to logarithmic.  
- Handles wildcard receives via separate tries/lists.  
- Code file: [`MPI_Radix_Trees`](./MPI_Radix_Trees)

---

## Compilation and Execution

### CPU-Based Matching
g++ CPU_Based_Matching.cpp -o cpu_matching -std=c++11 -pthread  
./cpu_matching

### Optimistic Tag Matching
g++ Optimistic_Tag_Matching.cpp -o optimistic_matching -std=c++11 -pthread  
./optimistic_matching

### Radix Trie–Based Matching
g++ MPI_Radix_Trees.cpp -o radix_matching -std=c++11 -pthread  
./radix_matching

> All programs assume the input message trace file (`input_trace.txt`) is present in the same directory.

---

## Comparison and Results

- **CPU-Based:** Simple and deterministic; suitable for small workloads.  
- **Optimistic Matching:** Multi-threaded for higher throughput; may introduce synchronization overhead.  
- **Radix Trie Matching:** Efficient lookup, scalable, and maintains correctness; performs best on large workloads.  

---

## Conclusion

This project explores progressively advanced strategies for MPI message matching. Key insights:

- **Baseline CPU matching** is reliable but slow for large workloads.  
- **Optimistic matching** improves throughput with concurrency.  
- **Radix trie matching** combines efficiency with correctness, offering significant speedup in large-scale simulations.

---

## Acknowledgment

We thank **Dr. Jagpreet Singh** for guidance and support, and the **team members** for consistent collaboration and testing.

---

**Team Members:**

- Aadit Mahajan (2023CSB1091)  
- Aksh Sihag (2023CSB1097)  
- Rahul Goyal (2023CSB1150)  
