#!/usr/bin/env python3
"""
UUID Searcher - Search all local drives for usr_ UUID patterns
Uses async I/O and multithreading for optimal performance
"""

import asyncio
import os
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Set, Tuple
import time
import sys

# UUID pattern: usr_ followed by standard UUID format
UUID_PATTERN = re.compile(rb'usr_[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE)

class UUIDSearcher:
    def __init__(self, max_workers: int = None, chunk_size: int = 1024 * 1024):  # 1MB chunks
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
        self.chunk_size = chunk_size
        self.found_uuids: Set[bytes] = set()
        self.lock = threading.Lock()
        self.processed_files = 0
        self.total_files = 0
        self.output_file = None
        self.written_uuids: Set[str] = set()
        
    def get_local_drives(self) -> List[Path]:
        """Get all local drives on Windows"""
        drives = []
        for drive_letter in 'ABCDEFGHIJKLMNOPQRSTUVWXYZ':
            drive_path = Path(f"{drive_letter}:\\")
            if drive_path.exists() and drive_path.is_dir():
                try:
                    # Check if it's a local drive (not network, removable, etc.)
                    drive_type = os.system(f'wmic logicaldisk where "DeviceID=\'{drive_letter}:\'" get DriveType /value 2>nul')
                    drives.append(drive_path)
                except:
                    drives.append(drive_path)
        return drives
    
    def is_huge_file(self, file_path: Path) -> bool:
        """Check if file is over 10GB"""
        try:
            return file_path.stat().st_size > 10 * 1024 * 1024 * 1024  # 10GB
        except:
            return True
    
    def search_file_for_uuids(self, file_path: Path) -> List[bytes]:
        """Search a single file for UUID patterns"""
        found_uuids = []
        
        try:
            # Skip huge files (>10GB) to avoid memory issues
            if self.is_huge_file(file_path):
                return found_uuids
                
            with open(file_path, 'rb') as f:
                # Read file in chunks to handle large files
                while True:
                    chunk = f.read(self.chunk_size)
                    if not chunk:
                        break
                    
                    # Search for UUID pattern in chunk
                    matches = UUID_PATTERN.findall(chunk)
                    found_uuids.extend(matches)
                    
                    # If we found matches, we can break early
                    if matches:
                        break
                        
        except (PermissionError, OSError, MemoryError):
            # Skip files we can't read
            pass
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            
        return found_uuids
    
    def write_uuid_to_file(self, uuid_bytes: bytes, output_file: str = "user_ids.txt"):
        """Write a UUID to file if it hasn't been written before"""
        # Convert bytes to string and remove 'usr_' prefix
        uuid_str = uuid_bytes.decode('utf-8', errors='ignore')
        # if uuid_str.startswith('usr_'):
        #     uuid_str = uuid_str[4:]  # Remove 'usr_' prefix
        
        with self.lock:
            # Check if we've already written this UUID
            if uuid_str not in self.written_uuids:
                self.written_uuids.add(uuid_str)
                
                # Open file in append mode and write the UUID
                with open(output_file, 'a', encoding='utf-8') as f:
                    f.write(f"{uuid_str}\n")
    
    def process_directory(self, directory: Path) -> List[Path]:
        """Get all files in a directory (recursively)"""
        files = []
        try:
            for item in directory.rglob('*'):
                if item.is_file():
                    files.append(item)
        except (PermissionError, OSError):
            # Skip directories we can't access
            pass
        return files
    
    def search_drive(self, drive_path: Path) -> List[bytes]:
        """Search all files on a single drive"""
        print(f"Scanning drive {drive_path}...")
        drive_uuids = []
        
        # Get all files on the drive
        all_files = self.process_directory(drive_path)
        self.total_files += len(all_files)
        
        print(f"Found {len(all_files)} files on {drive_path}")
        
        # Process files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all file search tasks
            future_to_file = {
                executor.submit(self.search_file_for_uuids, file_path): file_path 
                for file_path in all_files
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    file_uuids = future.result()
                    if file_uuids:
                        drive_uuids.extend(file_uuids)
                        # Write UUIDs to file immediately
                        for uuid in file_uuids:
                            self.write_uuid_to_file(uuid)
                        
                    self.processed_files += 1
                    if self.processed_files % 1000 == 0:
                        print(f"Processed {self.processed_files}/{self.total_files} files... Found {len(self.written_uuids)} unique UUIDs so far...")
                        
                except Exception as e:
                    print(f"Error processing {file_path}: {e}")
        
        return drive_uuids
    
    def search_all_drives(self) -> Set[bytes]:
        """Search all local drives for UUID patterns in parallel"""
        drives = self.get_local_drives()
        print(f"Found {len(drives)} local drives: {[str(d) for d in drives]}")
        
        all_uuids = set()
        
        # Process all drives in parallel
        with ThreadPoolExecutor(max_workers=len(drives)) as executor:
            # Submit all drive search tasks
            future_to_drive = {
                executor.submit(self.search_drive, drive): drive 
                for drive in drives
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_drive):
                drive = future_to_drive[future]
                try:
                    drive_uuids = future.result()
                    all_uuids.update(drive_uuids)
                    print(f"Completed scanning {drive} - Found {len(drive_uuids)} UUIDs")
                except Exception as e:
                    print(f"Error scanning drive {drive}: {e}")
        
        return all_uuids
    
    def initialize_output_file(self, output_file: str = "user_ids.txt"):
        """Initialize the output file (clear it at start)"""
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("")  # Clear the file
        print(f"Initialized output file: {output_file}")
    
    def get_final_count(self, output_file: str = "user_ids.txt"):
        """Get the final count of written UUIDs"""
        return len(self.written_uuids)

def main():
    print("UUID Searcher - Searching all local drives for usr_ UUID patterns")
    print("=" * 60)
    
    start_time = time.time()
    
    # Create searcher instance
    searcher = UUIDSearcher()
    
    try:
        # Initialize output file
        searcher.initialize_output_file()
        
        # Search all drives
        found_uuids = searcher.search_all_drives()
        
        end_time = time.time()
        duration = end_time - start_time
        
        print("=" * 60)
        print(f"Search completed in {duration:.2f} seconds")
        print(f"Processed {searcher.processed_files} files")
        print(f"Found {searcher.get_final_count()} unique UUIDs")
        print(f"Results written to user_ids.txt during search")
        
    except KeyboardInterrupt:
        print("\nSearch interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Error during search: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
