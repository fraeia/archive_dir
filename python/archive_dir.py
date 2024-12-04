import os
import subprocess
import argparse
from datetime import datetime
from tqdm import tqdm
import sqlite3
import mimetypes
import uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from PIL import Image
from moviepy import VideoFileClip
import cairosvg
import time
import warnings
import sys
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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

def generate_thumbnail(file_path, content_type, conn):
    thumbnail = None
    try:
        if content_type.startswith('image/'):
            if file_path.lower().endswith('.svg'):
                # Convert SVG to PNG
                png_file_path = file_path + '.png'
                cairosvg.svg2png(url=file_path, write_to=png_file_path)
                file_path = png_file_path
            
            with Image.open(file_path) as img:
                img.thumbnail((128, 128))
                thumbnail = img.tobytes()
        elif content_type.startswith('video/'):
            with VideoFileClip(file_path) as video:
                frame = video.get_frame(1)
                img = Image.fromarray(frame)
                img.thumbnail((128, 128))
                thumbnail = img.tobytes()
    except Exception as e:
        log_event("Error generating thumbnail", file_path, str(e), conn)
    return thumbnail

def log_event(event_type, file_path, message, conn):
    retries = 5
    while retries > 0:
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO events (id, event_type, file_path, message, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (str(uuid.uuid4()), event_type, file_path, message, datetime.now().isoformat()))
            conn.commit()
            break
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                retries -= 1
                time.sleep(1)
            else:
                raise

def compress_files_and_save_to_db(src_dir, dest_dir, conn, timestamp):
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY,
            filename TEXT,
            filepath TEXT,
            content_type TEXT,
            size INTEGER,
            creation_time TEXT,
            modification_time TEXT,
            thumbnail BLOB,
            is_duplicate INTEGER,
            original_path TEXT,
            batch TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            event_type TEXT,
            file_path TEXT,
            message TEXT,
            timestamp TEXT
        )
    ''')
    
    # Count total files and directories for progress bar
    total_items = sum(len(files) + len(dirs) for _, dirs, files in os.walk(src_dir))
    
    with tqdm(total=total_items, desc="Processing items", unit="item") as pbar:
        for root, dirs, files in os.walk(src_dir):
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                relative_path = os.path.relpath(root, src_dir)
                dest_path = os.path.join(dest_dir, relative_path)
                
                if not os.path.exists(dest_path):
                    os.makedirs(dest_path)
                
                # Save directory record to database
                original_filename = dir
                stat = os.stat(dir_path)
                size = stat.st_size
                creation_time = datetime.fromtimestamp(stat.st_ctime).isoformat()
                modification_time = datetime.fromtimestamp(stat.st_mtime).isoformat()
                
                cursor.execute('''
                    INSERT INTO files (id, filename, filepath, content_type, size, creation_time, modification_time, thumbnail, is_duplicate, original_path, batch)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (str(uuid.uuid4()), original_filename, os.path.join(relative_path, dir).replace("\\", "/"), "directory", size, creation_time, modification_time, None, 0, dir_path, timestamp))
                
                pbar.update(1)
            
            for file in files:
                file_path = os.path.join(root, file)
                relative_path = os.path.relpath(root, src_dir)
                dest_path = os.path.join(dest_dir, relative_path)
                
                if not os.path.exists(dest_path):
                    os.makedirs(dest_path)
                
                try:
                    # Check for duplicates in the database
                    original_filename = file
                    content_type, _ = mimetypes.guess_type(file_path)
                    stat = os.stat(file_path)
                    size = stat.st_size
                    creation_time = datetime.fromtimestamp(stat.st_ctime).isoformat()
                    modification_time = datetime.fromtimestamp(stat.st_mtime).isoformat()
                    
                    cursor.execute('''
                        SELECT filepath FROM files WHERE filename = ? AND size = ? AND creation_time = ? AND modification_time = ?
                    ''', (original_filename, size, creation_time, modification_time))
                    original_record = cursor.fetchone()
                    is_duplicate = original_record is not None
                    
                    if is_duplicate:
                        original_path = original_record[0]
                        cursor.execute('''
                            INSERT INTO files (id, filename, filepath, content_type, size, creation_time, modification_time, thumbnail, is_duplicate, original_path, batch)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (str(uuid.uuid4()), original_filename, os.path.join(relative_path, file + '.7z').replace("\\", "/"), content_type, size, creation_time, modification_time, None, 1, original_path, timestamp))
                        pbar.update(1)
                        continue
                    
                    # Compress the file with fastest compression
                    zip_file_path = os.path.join(dest_path, file + '.7z')
                    compression_level = os.getenv('COMPRESSION_LEVEL', '5')
                    subprocess.run(['7z', 'a', '-t7z', f'-mx={compression_level}', '-m0=LZMA2', '-md=32m', '-ms=64m', '-mmt=4', '-bd', zip_file_path, file_path], 
                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
                    
                    # Generate thumbnail
                    thumbnail = generate_thumbnail(file_path, content_type, conn) if content_type and (content_type.startswith('image/') or content_type.startswith('video/')) else None
                    
                    # Save file record to database
                    cursor.execute('''
                        INSERT INTO files (id, filename, filepath, content_type, size, creation_time, modification_time, thumbnail, is_duplicate, original_path, batch)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (str(uuid.uuid4()), original_filename, os.path.join(relative_path, file + '.7z').replace("\\", "/"), content_type, size, creation_time, modification_time, thumbnail, 0, file_path, timestamp))
                    
                    pbar.update(1)
                except Exception as e:
                    log_event("Error processing file", file_path, str(e), conn)
    
    conn.commit()

def upload_to_azure(container_name, connection_string, dest_dir, timestamp, conn):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    cursor = conn.cursor()
    
    # Count total files for progress bar
    cursor.execute('SELECT COUNT(*) FROM files WHERE batch = ? AND is_duplicate = 0', (timestamp,))
    total_files = cursor.fetchone()[0]
    
    with tqdm(total=total_files, desc="Uploading to Azure", unit="file") as pbar:
        cursor.execute('SELECT filepath FROM files WHERE batch = ? AND is_duplicate = 0 AND content_type != "directory"', (timestamp,))
        for row in cursor.fetchall():
            file_path = row[0]
            # blob_path = os.path.join(timestamp, os.path.relpath(file_path, dest_dir)).replace("\\", "/")

            local_file = (dest_dir + '/' + file_path).replace("\\", "/")
            blob_file = (timestamp + '/' + file_path).replace("\\", "/")
            blob_client = container_client.get_blob_client(blob_file)
            
            try:
                print(f"Uploading: {local_file} -> {blob_file}")
                with open(local_file, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                    pbar.update(1)
            except Exception as e:
                log_event("Error uploading file to Azure", file_path, str(e), conn)

def remove_directory(directory):
    for root, dirs, files in os.walk(directory, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            os.rmdir(os.path.join(root, dir))
    os.rmdir(directory)

def custom_warning_handler(message, category, filename, lineno, file=None, line=None):
    if "ffmpeg_reader.py" in filename:
        log_event("FFmpeg Warning", filename, str(message), conn)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compress files in a directory individually and save metadata to a database.")
    parser.add_argument("--src_directory", help="Source directory to compress files from", 
                        default=os.getenv('SOURCE_DIRECTORY'))
    parser.add_argument("--dest_directory", help="Destination directory to save compressed files",
                        default=os.getenv('DESTINATION_DIRECTORY'))
    parser.add_argument("--db_path", help="Path to the SQLite database file",
                        default=os.getenv('DB_PATH'))
    parser.add_argument("--azure_container", help="Azure Blob Storage container name", 
                        default=os.getenv('AZURE_CONTAINER'))
    parser.add_argument("--azure_connection_string", help="Azure Blob Storage connection string", 
                        default=os.getenv('AZURE_CONNECTION_STRING'))
    
    args = parser.parse_args()
    
    start_time = datetime.now()
    print(f"Process started at: {start_time}")
    
    original_size = get_directory_size(args.src_directory)
    
    # Generate a timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Open a single database connection
    conn = sqlite3.connect(args.db_path)
    
    # Set custom warning handler
    warnings.showwarning = custom_warning_handler
    
    try:
        # Compress files and save metadata to the database
        compress_files_and_save_to_db(args.src_directory, args.dest_directory, conn, timestamp)
        
        compressed_size = get_directory_size(args.dest_directory)
        
        end_time = datetime.now()
        print(f"Process ended at: {end_time}")
        print(f"Total duration: {end_time - start_time}")
        print(f"Total original size: {format_size(original_size)}")
        print(f"Total compressed size: {format_size(compressed_size)}")
        
        print(f"Directory tree saved to database: {args.db_path}")
        
        # Upload to Azure Blob Storage if specified
        if args.azure_container and args.azure_connection_string:
            upload_to_azure(args.azure_container, args.azure_connection_string, args.dest_directory, timestamp, conn)
            print(f"Files uploaded to Azure Blob Storage container: {args.azure_container}")
            
            # Remove the created destination files and directory
            remove_directory(args.dest_directory)
            print(f"Removed destination directory: {args.dest_directory}")
    finally:
        conn.close()
        warnings.showwarning = warnings._showwarnmsg_impl  # Reset warning handler to default