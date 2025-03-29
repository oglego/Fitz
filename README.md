# Fitz: Benchmarking Parquet Writing in Rust and Python

## Introduction
Fitz is a performance benchmarking project that compares the speed of writing Parquet files between Rust and Python. 

For this analysis, we will be working with synthetic data housed in a mysql database - the schema structure of the table is as follows:

    name VARCHAR(255) NOT NULL
    age BIGINT NOT NULL
    salary DOUBLE NOT NULL
    city VARCHAR(255) NOT NULL
    is_employed VARCHAR(5) NOT NULL

The data was generated and loaded into the table using Rust, but for this analysis we will just be focusing on the write times of Python and Rust.

## Project Overview
Fitz runs a structured experiment where both Rust and Python:
1. Query a table of **10,000,000 rows** from a **MySQL database**.
2. Write the retrieved data to a **Parquet file** using Apache Arrow.
3. Measure and compare the time taken for the entire process.

This comparison provides insights into performance differences between Rust and Python when handling large-scale data serialization.

## Technologies Used
- **Rust**: For high-performance data processing.
- **Python**: A widely used language for data science and analytics.
- **Apache Arrow**: Columnar data format for fast in-memory analytics.
- **MySQL**: The relational database serving as the data source.
- **Parquet**: The target format for efficient data storage and retrieval.

## How It Works
1. **Setup the MySQL Database**
   - Ensure a table with 10 million rows exists in your MySQL instance.
   - Provide database connection details in the configuration file.
   
2. **Run the Rust and Python Scripts**
   - The Rust implementation queries MySQL and writes to Parquet using Arrow.
   - The Python implementation does the same using Pandas and PyArrow.
   
3. **Benchmarking**
   - The execution time of each implementation is recorded and compared.

## Running the Benchmark
### Prerequisites
- Install Rust and Cargo.
- Install Python and the required libraries (`pandas`, `pyarrow`, `pymysql`).
- A running MySQL instance with the test dataset.

### Running the Rust Version
```sh
cargo run --release
```

### Running the Python Version
```sh
python fitz_python.py
```

### Viewing the Results
- Execution times will be printed to the console.
- You can analyze the differences to understand performance trade-offs.

## Results & Analysis
The results help determine:
- Which language is faster for large-scale data extraction and writing.
- The efficiency gains of Rust’s memory safety and performance optimizations.
- The trade-offs between development time and execution speed.

## Findings
For rust we obtained:

Query execution time: 25.528717125s

Data conversion time: 3.66461275s

Parquet write time: 13.579257416s

Total execution time: 42.772587291s

For python we obtained:

Query execution time: 49.33 seconds

Parquet writing time: 4.53 seconds

Total execution time: 53.87 seconds

By total execution time there is a ~23% difference between write times!

## Why "Fitz"?
Fitz, inspired by F. Scott Fitzgerald, symbolizes **efficient writing**—both in literature and in data processing. Just as Fitzgerald crafted elegant prose, this project aims to evaluate and optimize elegant, high-performance data writing.


