mod rquery;
fn main() {

    // Main function to query a MySQL database and save the result as a parquet file.

    // This function defines the database connection url, sql query, and output file path - the
    // main function then calls the `rquery::mysql_to_parquet` function to execute the query
    // and write the result to parquet.
     
    let mysql_url = "mysql://thisisausername:thisisapassword@localhost:3306/RUST_TEST";
    let query = "SELECT * FROM RUST_TEST.salary_data";
    let output_file = "salary_data.parquet";

    println!("Querying MySQL and writing to Parquet...");
    rquery::mysql_to_parquet(mysql_url, query, output_file);

}
