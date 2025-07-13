# 🦀 Solve Fly.io Challenge

This repository contains a simple yet effective Rust implementation for solving the [Fly.io Distributed Systems Challenges](https://fly.io/dist-sys/).

## 🚀 Overview

This project is a series of distributed systems solutions written in Rust, focusing on reliability, concurrency, and message passing.

### 🛰️ Implemented Challenges

1. **Broadcast**  
   Implements a gossip-based broadcast protocol using vector clocks, inspired by Cassandra's anti-entropy mechanisms. Ensures eventual consistency among nodes through peer-to-peer message propagation.

2. **Kafka-Log**  
   Implements a lightweight Kafka-style commit log using a `linkv`-style approach, supporting ordered, append-only logs with offset-based reads.


### 🛠️ Run

Run against Fly.io test framework:
```bash
cargo build --release
./maelstrom test -w broadcast --bin ./target/release/broadcast --node-count 25 --time-limit 20 --rate 100 --nemesis partition --latency 100
```