
# C++ Based DDL Interpreter for Database Management System

This project is a command-line processor that interprets Data Definition Language (DDL) statements for a database-like environment. It provides functionality to create, manage, and manipulate tables and records through user-supplied commands.

## Features

- Supports creating and dropping tables.
- Allows the insertion, deletion, and selection of records.
- Supports basic DDL commands such as `CREATE TABLE`, `DROP TABLE`, `INSERT INTO`, `DELETE FROM`, and `SELECT`.
- Implements filtering and sorting for `SELECT` queries.

## Getting Started

### Prerequisites

- A C++ compiler (e.g., GCC or Clang)
- Make sure you have standard libraries such as `stdio.h`, `stdlib.h`, and `string.h`.

### Compilation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-username/command-line-ddl.git
   ```
2. Navigate to the project directory:
   ```bash
   cd command-line-ddl
   ```
3. Compile the program using a C++ compiler:
   ```bash
   g++ final-1.cpp -o clp-processor
   ```

### Usage

Once compiled, the program can be executed with a single DDL statement as an argument:

```bash
./clp-processor "CREATE TABLE my_table (id INT, name VARCHAR(100));"
```

This command will create a table named `my_table` with two columns: `id` (integer) and `name` (string).

You can use other supported commands like:

- **Create Table**:
  ```bash
  ./clp-processor "CREATE TABLE table_name (column_name data_type, ...);"
  ```

- **Drop Table**:
  ```bash
  ./clp-processor "DROP TABLE table_name;"
  ```

- **Insert Record**:
  ```bash
  ./clp-processor "INSERT INTO table_name VALUES (value1, value2, ...);"
  ```

- **Delete Record**:
  ```bash
  ./clp-processor "DELETE FROM table_name WHERE condition;"
  ```

- **Select Record**:
  ```bash
  ./clp-processor "SELECT * FROM table_name WHERE condition ORDER BY column_name;"
  ```

### Files

- `final-1.cpp`: Contains the main logic and command-line processing functions.
- `final-1.h`: Header file that includes structure definitions and function prototypes used throughout the project.

## Contributing

If you would like to contribute to this project, feel free to fork the repository and submit a pull request.

## License

This project is licensed under the MIT License.
