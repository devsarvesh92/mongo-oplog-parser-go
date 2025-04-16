# Mongo Oplog Parser

A tool to parse MongoDB oplog files and convert them into SQL statements.

## Getting Started

### Prerequisites

- Install [Go](https://golang.org/dl/) 1.18 or later.

### Clone the Repository

```bash
git clone https://github.com/your-username/mongo-oplog-parser-go.git
cd mongo-oplog-parser-go
```

### Build and Install the Project

```bash
make install
```

This will build the project and install the binary to your system's `$GOPATH/bin` directory.

### Run the Parser

```bash
mongo-oplog-parser -f <path-to-oplog-file> -o <path-to-output-sql-file> -t mongo-file -w file
```

### Example

```bash
mongo-oplog-parser -f ./examples/sample.json -o ./output.sql -t mongo-file -w file
```

### Command Options:
- `-f`: Path to the input oplog file (e.g., `./examples/sample.json`).
- `-o`: Path to the output SQL file (e.g., `./output.sql`).
- `-t`: Input type (e.g., `mongo-file`).
- `-w`: Output writer type (e.g., `file`).

## Makefile Commands

- **Build**:  
    ```bash
    make build
    ```

- **Install**:  
    ```bash
    make install
    ```

- **Run Example**:  
    ```bash
    make run-example
    ```

- **Clean**:  
    ```bash
    make clean
    ```

## License

This project is licensed under the MIT License.Desitination