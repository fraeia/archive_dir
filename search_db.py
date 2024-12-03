import sqlite3
import argparse

def format_size(size):
    # Convert size to human-readable format
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024

def search_database(db_path, filter_criteria):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Select only the required columns in the desired order
    query = "SELECT id, filename, content_type, size, filepath FROM files WHERE "
    query += " AND ".join([f"{key} LIKE ?" for key in filter_criteria.keys()])
    values = [f"%{value}%" for value in filter_criteria.values()]
    
    cursor.execute(query, values)
    results = cursor.fetchall()
    
    conn.close()
    return results

if __name__ == "__main__":
    # Temporarily set db_path and filename for debugging
    db_path = r"C:\Temp\directory_tree.db"
    filter_criteria = {"filename": "%savills%"}  # Using wildcard to match any filename containing 'package'
    
    results = search_database(db_path, filter_criteria)
    
    # Print rows
    for row in results:
        row = list(row)
        row[3] = format_size(row[3])  # Format the size column
        print("\t".join(map(str, row)))
    
    # Uncomment the following lines to use command-line arguments instead
    # parser = argparse.ArgumentParser(description="Search the database for records matching the filter criteria.")
    # parser.add_argument("db_path", help="Path to the SQLite database file")
    # parser.add_argument("--filename", help="Filter by filename")
    # parser.add_argument("--filepath", help="Filter by filepath")
    # parser.add_argument("--content_type", help="Filter by content type")
    # parser.add_argument("--size", help="Filter by size")
    # parser.add_argument("--creation_time", help="Filter by creation time")
    # parser.add_argument("--modification_time", help="Filter by modification time")
    
    # args = parser.parse_args()
    
    # filter_criteria = {key: value for key, value in vars(args).items() if key != "db_path" and value is not None}
    
    # results = search_database(args.db_path, filter_criteria)
    
    # Print rows
    # for row in results:
    #     row = list(row)
    #     row[3] = format_size(row[3])  # Format the size column
    #     print("\t".join(map(str, row)))