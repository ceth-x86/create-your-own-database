# Build Your Own Database

A simple key-value database implementation in Go that uses a B+ tree data structure for efficient storage and retrieval.

## Features

- B+ tree implementation for efficient key-value storage
- Persistent storage on disk
- Thread-safe operations
- Basic CRUD operations (Create, Read, Update, Delete)
- Range traversal support

## Project Structure

```
.
├── cmd/
│   └── db/
│       └── main.go         # Main program demonstrating usage
├── pkg/
│   ├── btree/
│   │   ├── node.go        # BNode implementation
│   │   └── tree.go        # BTree implementation
│   ├── storage/
│   │   └── storage.go     # Disk storage implementation
│   └── db/
│       └── db.go          # High-level database interface
└── README.md
```

## Usage

```go
import "build-your-own-database/pkg/db"

// Create a new database
database, err := db.NewDB("data/db")
if err != nil {
    log.Fatal(err)
}
defer database.Close()

// Insert a key-value pair
err = database.Put([]byte("key"), []byte("value"))

// Retrieve a value
value, found := database.Get([]byte("key"))

// Delete a key
err = database.Delete([]byte("key"))

// Traverse all key-value pairs
database.Traverse(func(key, value []byte) {
    fmt.Printf("%s -> %s\n", string(key), string(value))
})
```

## Implementation Details

### B+ Tree Structure

The database uses a B+ tree data structure where:
- Internal nodes contain only keys and pointers to child nodes
- Leaf nodes contain key-value pairs
- All leaf nodes are linked together for efficient range queries
- The tree is balanced to maintain O(log n) operations

### Storage

Data is persisted to disk using a simple file-based storage system:
- Each node is stored as a fixed-size page
- Pages are written sequentially to disk
- The file is memory-mapped for efficient access

## Building and Running

```bash
# Build the project
go build -o db cmd/db/main.go

# Run the example
./db
```

## License

MIT License 