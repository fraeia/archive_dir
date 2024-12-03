import os
import subprocess
import argparse
from datetime import datetime
from tqdm import tqdm
import sqlite3
import mimetypes
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from PIL import Image
from moviepy import VideoFileClip
import cairosvg

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

def generate_thumbnail(file_path, content_type):
    thumbnail = None
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
        try:
            with VideoFileClip(file_path) as video:
                frame = video.get_frame(1)
                img = Image.fromarray(frame)
                img.thumbnail((128, 128))
                thumbnail = img.tobytes()
        except Exception as e:
            print(f"Error generating thumbnail for video file {file_path}: {e}")
    return thumbnail

def generate_directory_tree_to_db(src_dir, dest_dir, db_path, timestamp):
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
            modification_time TEXT,
            thumbnail BLOB
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
                INSERT INTO files (filename, filepath, content_type, size, creation_time, modification_time, thumbnail)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (dir, os.path.join(timestamp, relative_path).replace("\\", "/"), "directory", size, creation_time, modification_time, None))
        
        for file in files:
            dest_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(root, dest_dir)
            src_file_path = os.path.join(src_dir, relative_path, file.replace('.7z', ''))
            original_filename = file.replace('.7z', '')  # Remove the .7z extension to get the original filename
            content_type, _ = mimetypes.guess_type(src_file_path)
            stat = os.stat(dest_file_path)
            size = stat.st_size
            creation_time = datetime.fromtimestamp(stat.st_ctime).isoformat()
            modification_time = datetime.fromtimestamp(stat.st_mtime).isoformat()
            thumbnail = generate_thumbnail(src_file_path, content_type) if content_type and (content_type.startswith('image/') or content_type.startswith('video/')) else None
            cursor.execute('''
                INSERT INTO files (filename, filepath, content_type, size, creation_time, modification_time, thumbnail)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (original_filename, os.path.join(timestamp, relative_path, file).replace("\\", "/"), content_type, size, creation_time, modification_time, thumbnail))
    
    conn.commit()
    conn.close()

def upload_to_azure(container_name, connection_string, dest_dir, timestamp):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)
    
    # Count total files for progress bar
    total_files = sum(len(files) for _, _, files in os.walk(dest_dir))
    
    with tqdm(total=total_files, desc="Uploading to Azure", unit="file") as pbar:
        for root, dirs, files in os.walk(dest_dir):
            for file in files:
                file_path = os.path.join(root, file)
                blob_path = os.path.join(timestamp, os.path.relpath(file_path, dest_dir)).replace("\\", "/")
                blob_client = container_client.get_blob_client(blob_path)
                
                with open(file_path, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
                    pbar.update(1)

def remove_directory(directory):
    for root, dirs, files in os.walk(directory, topdown=False):
        for file in files:
            os.remove(os.path.join(root, file))
        for dir in dirs:
            os.rmdir(os.path.join(root, dir))
    os.rmdir(directory)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compress files in a directory individually and save metadata to a database.")
    parser.add_argument("src_directory", help="Source directory to compress files from")
    parser.add_argument("dest_directory", help="Destination directory to save compressed files")
    parser.add_argument("db_path", help="Path to the SQLite database file")
    parser.add_argument("--azure_container", help="Azure Blob Storage container name")
    parser.add_argument("--azure_connection_string", help="Azure Blob Storage connection string")
    
    args = parser.parse_args()
    
    start_time = datetime.now()
    print(f"Process started at: {start_time}")
    
    original_size = get_directory_size(args.src_directory)
    compress_files(args.src_directory, args.dest_directory)
    compressed_size = get_directory_size(args.dest_directory)
    
    end_time = datetime.now()
    print(f"Process ended at: {end_time}")
    print(f"Total duration: {end_time - start_time}")
    print(f"Total original size: {format_size(original_size)}")
    print(f"Total compressed size: {format_size(compressed_size)}")
    
    # Generate a timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    # Generate and save the destination directory tree to a database
    generate_directory_tree_to_db(args.src_directory, args.dest_directory, args.db_path, timestamp)
    print(f"Directory tree saved to database: {args.db_path}")
    
    # Upload to Azure Blob Storage if specified
    if args.azure_container and args.azure_connection_string:
        upload_to_azure(args.azure_container, args.azure_connection_string, args.dest_directory, timestamp)
        print(f"Files uploaded to Azure Blob Storage container: {args.azure_container}")
        
        # Remove the created destination files and directory
        remove_directory(args.dest_directory)
        print(f"Removed destination directory: {args.dest_directory}")