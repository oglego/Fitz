use std::fs::File;
use std::sync::Arc;
use std::time::Instant;
use mysql::*;
use mysql::prelude::*;
use arrow::array::{StringArray, Int64Array, Float64Array, BooleanArray, ArrayRef};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;

/// Queries a MySQL database and writes the result to a parquet file.
/// 
/// This function connects to the MySQL database using the provided URL, 
/// executes the given query, processes the results into Apache Arrow format, and
/// writes them to a parquet file.
/// 
/// # Arguments
/// * `mysql_url` - A string slice representing the MySQL connection URL.
/// * `query` - A string slice containing the sql query to execute.
/// * `parquet_file` - A string slice specifying the output parquet file path.
/// 
/// # Panics
/// This function panics if any of the following occur:
/// * The database connection fails.
/// * The query execution fails.
/// * The parquet file creation fails.
/// * The Arrow RecordBatch creation fails.
/// * The parquet writing process fails.
pub fn mysql_to_parquet(mysql_url: &str, query: &str, parquet_file: &str) {
    // Start measuring total execution time
    let start_time = Instant::now();

    // Connect to MySQL
    let pool = Pool::new(mysql_url).expect("Failed to create MySQL connection pool");
    let mut conn = pool.get_conn().expect("Failed to get MySQL connection");

    // Execute query and fetch results
    let result: Vec<(String, i64, f64, String, String)> = conn
        .query(query)
        .expect("Failed to execute query");

    // Capture query execution time
    let query_time = Instant::now(); 

    // Convert data into Arrow arrays
    let mut names = Vec::new();
    let mut ages = Vec::new();
    let mut salaries = Vec::new();
    let mut cities = Vec::new();
    let mut is_employed = Vec::new();

    for row in result {
        names.push(row.0);
        ages.push(row.1);
        salaries.push(row.2);
        cities.push(row.3);
        is_employed.push(row.4 == "true");  
    }

    // Create Arrow schema
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int64, false),
        Field::new("salary", DataType::Float64, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("is_employed", DataType::Boolean, false),
    ]));

    // Create Arrow RecordBatch
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(names)) as ArrayRef,
            Arc::new(Int64Array::from(ages)) as ArrayRef,
            Arc::new(Float64Array::from(salaries)) as ArrayRef,
            Arc::new(StringArray::from(cities)) as ArrayRef,
            Arc::new(BooleanArray::from(is_employed)) as ArrayRef,
        ],
    ).expect("Failed to create RecordBatch");

    let conversion_time = Instant::now(); // Capture conversion execution time

    // Write to Parquet file
    let file = File::create(parquet_file).expect("Failed to create Parquet file");
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).expect("Failed to create Parquet writer");

    writer.write(&batch).expect("Failed to write batch to Parquet");
    writer.close().expect("Failed to close Parquet writer");

    println!("Query results saved to {}", parquet_file);

    let end_time = Instant::now(); // Capture total execution time

    println!("Query execution time: {:?}", query_time.duration_since(start_time));
    println!("Data conversion time: {:?}", conversion_time.duration_since(query_time));
    println!("Parquet write time: {:?}", end_time.duration_since(conversion_time));
    println!("Total execution time: {:?}", end_time.duration_since(start_time));
}
