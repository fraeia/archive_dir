import os
import subprocess
import argparse
from datetime import datetime
from tqdm import tqdm
import sqlite3
import mimetypes

def get_directory_size(directory):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(directory):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            total_size += os.path.getsize(fp)
    return total_size

def format_size(size):
    # Convert size to human-readable format
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024

def compress_files(src_dir, dest_dir):
    # Count total files for progress bar
    total_files = sum(len(files) for _, _, files in os.walk(src_dir))
    
    with tqdm(total=total_files, desc="Compressing files", unit="file") as pbar:
        for root, dirs, files in os.walk(src_dir):
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(root, src_dir)
                dest_path = os.path.join(dest_dir, relative_path)
                
                if not os.path.exists(dest_path):
                    os.makedirs(dest_path)
                
                zip_file_path = os.path.join(dest_path, file + '.7z')
                subprocess.run(['7z', 'a', '-t7z', '-mx=5', '-m0=LZMA2', '-md=32m', '-ms=64m', '-mmt=4', '-bd', zip_file_path, file_path], 
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
                
                pbar.update(1)

def generate_directory_tree_to_db(src_dir, dest_dir, db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY,
            filename TEXT,
            filepath TEXT,
            content_type TEXT,
            size INTEGER,
            creation_time TEXT,
            modification_time TEXT
        )
    ''')
    
    # Insert file and directory information into the table
    for root, dirs, files in os.walk(dest_dir):
        for dir in dirs:
            dir_path = os.path.join(root, dir)
            relative_path = os.path.relpath(dir_path, dest_dir)
            stat = os.stat(dir_path)
            size = stat.st_size
            creation_time = datetime.fromtimestamp(stat.st_ctime).isoformat()
            modification_time = datetime.fromtimestamp(stat.st_mtime).isoformat()
            cursor.execute('''
                INSERT INTO files (filename, filepath, content_type, size, creation_time, modification_time)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (dir, dir_path, "directory", size, creation_time, modification_time))
        
        for file in files:
            dest_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(root, dest_dir)
            src_file_path = os.path.join(src_dir, relative_path)
            original_filename = file.replace('.7z', '')  # Remove the .7z extension to get the original filename
            content_type, _ = mimetypes.guess_type(os.path.join(src_file_path, original_filename))
            stat = os.stat(dest_file_path)
            size = stat.st_size
            creation_time = datetime.fromtimestamp(stat.st_ctime).isoformat()
            modification_time = datetime.fromtimestamp(stat.st_mtime).isoformat()
            cursor.execute('''
                INSERT INTO files (filename, filepath, content_type, size, creation_time, modification_time)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (original_filename, dest_file_path, content_type, size, creation_time, modification_time))
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    # Temporarily set source and destination directories and database path for debugging
    src_directory = r"C:\Temp\Code"
    dest_directory = r"C:\Temp\Code7z"
    db_path = r"C:\Temp\directory_tree.db"
    
    start_time = datetime.now()
    print(f"Process started at: {start_time}")
    
    original_size = get_directory_size(src_directory)
    compress_files(src_directory, dest_directory)
    compressed_size = get_directory_size(dest_directory)
    
    end_time = datetime.now()
    print(f"Process ended at: {end_time}")
    print(f"Total duration: {end_time - start_time}")
    print(f"Total original size: {format_size(original_size)}")
    print(f"Total compressed size: {format_size(compressed_size)}")
    
    # Generate and save the destination directory tree to a database
    generate_directory_tree_to_db(src_directory, dest_directory, db_path)
    print(f"Directory tree saved to database: {db_path}")
    
    # Uncomment the following lines to use command-line arguments instead
    # parser = argparse.ArgumentParser(description="Compress files in a directory individually and save metadata to a database.")
    # parser.add_argument("src_directory", help="Source directory to compress files from")
    # parser.add_argument("dest_directory", help="Destination directory to save compressed files")
    # parser.add_argument("db_path", help="Path to the SQLite database file")
    # args = parser.parse_args()
    # compress_files(args.src_directory, args.dest_directory)
    # generate_directory_tree_to_db(args.src_directory, args.dest_directory, args.db_path)