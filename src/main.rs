use clap::{Arg, Command};
use regex::Regex;
use rusqlite::{Connection, ToSql};
use std::fs::{self, File};
use std::io::{self, BufRead};
use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command-line arguments
    let matches = Command::new("Log to SQLite")
        .version("1.0")
        .author("Brandon Leflar <Brandon.Leflar@Transcore.com>")
        .about("Parses log files in a directory, matches lines with a regex, and inserts results into an SQLite database")
        .arg(
            Arg::new("log_dir")
                .help("The directory containing log files")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("file_filter")
                .help("Substring to filter log file names")
                .required(true)
                .index(2),
        )
        .arg(
            Arg::new("db_path")
                .help("Path to the SQLite database file")
                .required(true)
                .index(3),
        )
        .arg(
            Arg::new("regex")
                .help("Regular expression with named groups")
                .required(true)
                .index(4),
        )
        .get_matches();

    let log_dir = matches.get_one::<String>("log_dir").unwrap();
    let file_filter = matches.get_one::<String>("file_filter").unwrap();
    let db_path = matches.get_one::<String>("db_path").unwrap();
    let regex_pattern = matches.get_one::<String>("regex").unwrap();

    // Compile the regex
    let regex = Regex::new(regex_pattern)?;

    // Extract named groups from the regex
    let mut column_names: Vec<_> = regex
        .capture_names()
        .flatten()
        .map(|name| name.to_string())
        .collect();

    column_names.push("filename".to_string()); // Add the filename column

    // Connect to SQLite database
    let mut conn = Connection::open(db_path)?;
    let create_table_query = format!(
        "CREATE TABLE IF NOT EXISTS log_data ({})",
        column_names
            .iter()
            .map(|name| format!("{} TEXT", name))
            .collect::<Vec<_>>()
            .join(", ")
    );
    conn.execute(&create_table_query, [])?;
    println!("Database table verified.");

    // Find matching files
    let log_files = find_matching_files(log_dir, file_filter)?;
    if log_files.is_empty() {
        println!("No files matching the filter '{}' were found in '{}'.", file_filter, log_dir);
        return Ok(());
    }
    println!("Found {} matching files.", log_files.len());

    let mut total_matches = 0;

    // Sequentially process each file
    for (index, file_path) in log_files.iter().enumerate() {
        println!(
            "Processing file {} of {}: {:?}",
            index + 1,
            log_files.len(),
            file_path
        );
        total_matches += process_file(file_path, &mut conn, &regex, &column_names)?;
    }

    println!("Log processing completed. Total matches found: {}", total_matches);
    Ok(())
}

/// Process a single file and insert matches into the database.
fn process_file(
    file_path: &Path,
    conn: &mut Connection,
    regex: &Regex,
    column_names: &[String],
) -> Result<usize, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let mut match_count = 0;

    let tx = conn.transaction()?; // Start a transaction

    let placeholders = column_names.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let insert_query = format!(
        "INSERT INTO log_data ({}) VALUES ({})",
        column_names.join(", "),
        placeholders
    );

    let filename = file_path.file_name().unwrap_or_default().to_string_lossy().to_string();

    for line in reader.lines() {
        let line = line?;
        if let Some(captures) = regex.captures(&line) {
            match_count += 1;

            // Collect named group values
            let mut values: Vec<String> = column_names
                .iter()
                .filter(|name| *name != "filename")
                .map(|name| captures.name(name).map(|m| m.as_str().to_string()).unwrap_or_default())
                .collect();

            values.push(filename.clone()); // Add the filename value

            let params: Vec<&dyn ToSql> = values.iter().map(|v| v as &dyn ToSql).collect();
            tx.execute(&insert_query, rusqlite::params_from_iter(params))?;
        }
    }

    tx.commit()?; // Commit the transaction
    println!("Processed file {:?}, Matches: {}", file_path, match_count);
    Ok(match_count)
}

/// Finds all files in the given directory containing the specified substring in their names.
fn find_matching_files(dir: &str, filter: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut matching_files = Vec::new();

    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            let file_name = path.file_name().unwrap_or_default().to_string_lossy();
            if file_name.contains(filter) {
                matching_files.push(path);
            }
        }
    }

    Ok(matching_files)
}
