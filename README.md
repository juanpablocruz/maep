# Merkle Anti-Entropy Protocol (MAEP)

This project implements a prototype of the **Merkle Anti-Entropy Protocol (MAEP)** — a distributed synchronization and reconciliation mechanism that uses Merkle trees to track node state and detect conflicts. The protocol is designed for an AP (Availability/Partition Tolerance) environment (per the CAP theorem) by allowing nodes to operate independently, reconcile differences asynchronously, and reconfigure the network when nodes go offline.

> **Disclaimer:** This project is an experimental prototype currently in development. While it includes basic JOIN, SYNC, DELTA SYNC, and PING operations, as well as a simple reconfiguration stub, it is not yet production‑ready.

## Table of Contents

- [Features](#features)
- [Project Structure](#project-structure)
- [Installation](#installation)
- [Usage](#usage)
- [Testing](#testing)
- [Next Steps to Improve the Code](#next-steps-to-improve-the-code)

## Features

- **Protocol Message Handling:**  
  Implements the MAEP message types (JOIN, SYNC, DELTA SYNC, PING, ID, ACK ID) using a fixed 6‑byte binary header.

- **Merkle Tree State Management:**  
  Uses a Merkle tree to store versioned key–value pairs. Conflict detection is based on timestamps with a simple last-write-wins policy.

- **Node Operations:**  
  Each node runs as an independent MAEP agent that can:
  - Join the network via a JOIN request.
  - Synchronize state with its successor (triggering a delta sync if differences are detected).
  - Monitor neighbor connectivity using a ping mechanism.
  - Initiate a (simulated) network reconfiguration when neighbors go offline.

## Project Structure

```
. 
├── cmd 
│ └── maepsim 
│ ├── main.go
├── internal 
│ ├── protocol 
│ │ ├── protocol.go  
│ │ └── protocol_test.go 
│ ├── merkle 
│ │ ├── merkle.go  
│ │ └── merkle_test.go 
│ ├── node 
│ │ ├── node.go 
│ │ └── node_test.go 
│ └── tui 
│  └── state.go 
└── go.mod
```

## Installation

1. **Clone the Repository:**

```bash
git clone https://github.com/juanpablocruz/maepsim.git
cd maepsim
```

2. **Install Dependencies:**
This project uses Go modules. Ensure your Go version is 1.16 or newer, then run:

```bash
go mod tidy
```

3. **Install gocui (if not automatically fetched):**

```bash
go get github.com/jroimartin/gocui
```

## Usage

To build and run the simulation with the TUI, run:

```bash
go run cmd/maepsim/main.go
```

## Testing

To run the unit tests, execute:

```bash
go test ./...
```

## Next Steps to Improve the Code

1. **Robust Network Reconfiguration:**
   - **Implement Full Cord Formation:**  
     Extend the stubbed `initiateReconfiguration()` to implement the complete "Chiral Cord Reconfiguration" process:
     - Each node in a disconnected segment should form a “cord” by exchanging unique IDs.
     - Nodes should broadcast their cord head IDs and engage in a leader election (using a Raft-like algorithm).
     - The elected leader should coordinate merging the segments into a single ring.
   - **Dynamic Reconfiguration:**  
     Allow nodes to continuously monitor connectivity and trigger reconfiguration when long-lasting disconnections are detected.

2. **Advanced Conflict Resolution:**
   - **Improve Conflict Detection:**  
     Move beyond the simple timestamp-based last-write-wins policy by integrating vector clocks or Conflict-free Replicated Data Types (CRDTs).
   - **Consensus Mechanism:**  
     Implement a consensus algorithm (e.g., Raft) to manage concurrent writes and conflicts, ensuring stronger data consistency across nodes.

3. **Enhanced Error Handling and Resilience:**
   - **Graceful Shutdown:**  
     Refactor the code to use cancellable contexts, allowing a clean shutdown of the simulation and background goroutines.
   - **Retry Strategies:**  
     Add retry and exponential backoff logic for network operations (pings, syncs) to better handle transient errors.

4. **Performance and Scalability:**
   - **Profiling and Optimization:**  
     Benchmark the system under load, profile performance, and optimize critical sections (e.g., Merkle tree reconciliation).
   - **Distributed Deployment:**  
     Test the protocol by deploying nodes on separate machines or containers to simulate real-world network conditions.

5. **User Interface Enhancements:**
   - **Improved TUI Visualization:**  
     Enhance the network graph in the TUI to visually represent the ring topology using box-drawing characters or even ASCII diagrams.
   - **Interactive Controls:**  
     Allow interactive triggers via keybindings (e.g., force a reconfiguration, simulate node failures) to better observe protocol behavior.

6. **Extensive Testing and Documentation:**
   - **Expand the Test Suite:**  
     Develop additional unit and integration tests covering edge cases, network partitions, and full reconfiguration scenarios.
   - **Detailed Documentation:**  
     Document the protocol design, state machine, message flows, and all underlying assumptions in detail.

7. **Security Improvements:**
   - **Secure Transport:**  
     Implement secure communication channels (e.g., TLS) for inter-node messaging.
   - **Node Authentication:**  
     Introduce mechanisms to authenticate nodes and control access to the network.

---
