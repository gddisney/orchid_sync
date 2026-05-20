# OrchidSync

OrchidSync is a distributed, secure search engine built with Go, designed for high-performance information retrieval using the Okapi BM25 ranking algorithm and an embedded B+ Tree database.

## Overview

The engine manages local document indexing and provides a mechanism for distributed search across a mesh network. Key features include:

* **Secure Networking**: Utilizes a peer-to-peer network layer for communication.
* **Persistent Storage**: Built for atomic, transaction-aware data persistence.
* **BM25 Ranking**: Implements the Okapi BM25 algorithm, featuring term frequency saturation and length normalization.
* **NLP Pipeline**: Built-in analyzer for tokenization, case normalization, and stop-word filtering.

## Architecture

The engine operates by intercepting documents, tokenizing them, and storing them in an inverted index where each term maps to a list of postings.

## Core Components

* **Engine**: The top-level wrapper that manages the database connection, the network node, and the scoring logic.
* **Indexer**: Bridges the NLP analyzer and the storage layer to perform atomic updates on the inverted index.
* **Search**: Processes queries, fetches posting lists, calculates document scores using BM25, and returns a ranked list of hits.
* **ScatterGather**: Enables distributed search by broadcasting queries to the peer mesh and merging results.

## Usage

### Initializing the Engine

```go
engine, err := NewEngine("/path/to/db", 9999)
if err != nil {
    log.Fatal(err)
}

```

### Indexing a Document

```go
err := engine.Index("doc-001", "The quick brown fox jumps over the lazy dog.")

```

### Performing a Search

```go
results, err := engine.Search("quick fox", 10)

```

## Testing

The package includes a suite of tests to ensure the integrity of the NLP pipeline, the BM25 mathematical implementation, and the engine's initialization routines. Run the tests using the standard Go toolchain:

```bash
go test -v ./...

```
