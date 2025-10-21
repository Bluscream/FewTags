#!/usr/bin/env python3
"""
Yoinker Detector - Standalone Python Script
Based on the C# StandaloneNotifier program
Checks user IDs against the yoinker detection service and outputs results to CSV
"""

import hashlib
import json
import csv
import asyncio
import aiohttp
import argparse
import os
import threading
from datetime import datetime, timedelta, UTC
from typing import Optional, Dict, Tuple, List


class RateLimiter:
    """Thread-safe rate limiter (15 requests per 60 seconds)"""
    
    def __init__(self, max_requests: int = 15, time_frame_seconds: int = 60):
        self.max_requests = max_requests
        self.time_frame_seconds = time_frame_seconds
        self.requests: List[datetime] = []
        self._lock = threading.Lock()
    
    def request_made(self):
        with self._lock:
            now = datetime.now(UTC)
            self.requests.append(now)
            if len(self.requests) > (self.max_requests + 25):
                self.requests.pop(0)
    
    def is_rate_limit_exceeded(self) -> bool:
        with self._lock:
            now = datetime.now(UTC)
            cutoff_time = now - timedelta(seconds=self.time_frame_seconds)
            recent_requests = [req for req in self.requests if req > cutoff_time]
            return len(recent_requests) >= self.max_requests
    
    async def wait_if_needed(self):
        while self.is_rate_limit_exceeded():
            await asyncio.sleep(0.25)


class YoinkerDetector:
    """Main class for detecting yoinkers"""
    
    def __init__(self, max_concurrent: int = 5):
        self.rate_limiter = RateLimiter()
        self.cache: Dict[str, Tuple[datetime, Optional[Dict]]] = {}
        self.base_url = "https://yd.just-h.party/"
        self.max_concurrent = max_concurrent
        self._cache_lock = threading.Lock()
        self._last_request_time = datetime.now(UTC)
        self._request_lock = threading.Lock()
    
    def _generate_hash(self, user_id: str) -> str:
        """Generate SHA256 hash of user ID"""
        return hashlib.sha256(user_id.encode('utf-8')).hexdigest()
    
    def _check_cache(self, user_id: str) -> Optional[Dict]:
        """Check if user ID is in cache and not expired"""
        with self._cache_lock:
            if user_id in self.cache:
                cached_time, result = self.cache[user_id]
                if datetime.now(UTC) - cached_time < timedelta(minutes=30):
                    return result
                else:
                    del self.cache[user_id]
            
            if len(self.cache) > 512:
                self.cache.clear()
            
            return None
    
    def _add_to_cache(self, user_id: str, result: Optional[Dict]):
        """Add result to cache"""
        with self._cache_lock:
            self.cache[user_id] = (datetime.now(UTC), result)
    
    def _save_to_404_file(self, user_id: str):
        """Save user ID to 404.txt file"""
        try:
            with open("404.txt", "a", encoding="utf-8") as f:
                f.write(f"{user_id}\n")
        except Exception as e:
            print(f"Error saving to 404.txt for {user_id}: {e}")
    
    def _load_404_ids(self) -> set:
        """Load existing 404 IDs from file"""
        if not os.path.exists("404.txt"):
            return set()
        
        try:
            with open("404.txt", "r", encoding="utf-8") as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            print(f"Error reading 404.txt: {e}")
            return set()
    
    def _save_json_response(self, user_id: str, response_data: Optional[Dict], save_empty: bool, yoinkers_dir: str):
        """Save JSON response to file"""
        if not save_empty and not response_data:
            return
            
        try:
            json_file = os.path.join(yoinkers_dir, f"{user_id}.json")
            with open(json_file, 'w', encoding='utf-8') as f:
                if response_data:
                    json.dump(response_data, f, indent=2, ensure_ascii=False)
                else:
                    json.dump({
                        "userId": user_id,
                        "userName": "",
                        "isYoinker": False,
                        "reason": "",
                        "year": "",
                        "status": "not_found"
                    }, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving JSON for {user_id}: {e}")
    
    def _append_csv_result(self, user_id: str, result: Optional[Dict], save_empty: bool, output_file: str, csv_lock: threading.Lock, csv_initialized: List[bool]):
        """Append result to CSV file"""
        with csv_lock:
            try:
                # Initialize CSV with header if not done yet
                if not csv_initialized[0]:
                    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile, delimiter=';')
                        writer.writerow(['UserId', 'UserName', 'Year', 'Reason'])
                    csv_initialized[0] = True
                
                # Append result to CSV
                with open(output_file, 'a', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile, delimiter=';')
                    
                    if result:
                        writer.writerow([
                            result.get('userId', user_id),
                            result.get('userName', ''),
                            result.get('year', ''),
                            result.get('reason', '')
                        ])
                    elif save_empty:
                        writer.writerow([user_id, '', '', 'Not found'])
                        
            except Exception as e:
                print(f"Error writing to CSV for {user_id}: {e}")
    
    async def _ensure_minimum_delay(self):
        """Ensure minimum 100ms delay between requests"""
        now = datetime.now(UTC)
        time_since_last = (now - self._last_request_time).total_seconds()
        if time_since_last < 0.1:
            await asyncio.sleep(0.1 - time_since_last)
        self._last_request_time = datetime.now(UTC)
    
    async def check_user(self, session: aiohttp.ClientSession, user_id: str, yoinkers_dir: str = "yoinkers", retry_count: int = 0) -> Optional[Dict]:
        """Check a single user ID against the yoinker detection service"""
        
        # Check for existing JSON file first
        json_file = os.path.join(yoinkers_dir, f"{user_id}.json")
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if data.get('isYoinker', False):
                        return data
                    else:
                        # Add to 404.txt if not already there
                        if user_id not in self._load_404_ids():
                            self._save_to_404_file(user_id)
                        return None
            except Exception as e:
                print(f"Error reading existing JSON for {user_id}: {e}")
        
        # Check cache
        cached_result = self._check_cache(user_id)
        if cached_result is not None:
            return cached_result
        
        # Make API request with retry logic
        print(f"Making API request for {user_id} (attempt {retry_count + 1}/4)")
        await self.rate_limiter.wait_if_needed()
        print(f"Rate limit check passed for {user_id}")
        
        try:
            url = f"{self.base_url}{self._generate_hash(user_id)}"
            timeout = aiohttp.ClientTimeout(total=10, connect=5)
            async with session.get(url, timeout=timeout) as response:
                self.rate_limiter.request_made()
                
                if response.status == 200:
                    try:
                        data = await response.json()
                        if data.get('isYoinker', False):
                            self._add_to_cache(user_id, data)
                            return data
                        else:
                            self._add_to_cache(user_id, None)
                            return None
                    except json.JSONDecodeError:
                        print(f"Invalid JSON response for user {user_id}")
                        return None
                
                elif response.status == 404:
                    self._add_to_cache(user_id, None)
                    self._save_to_404_file(user_id)
                    return None
                
                elif response.status == 429:
                    print(f"Rate limited! Waiting...")
                    await asyncio.sleep(60)
                    return await self.check_user(session, user_id, yoinkers_dir, retry_count)
                
                else:
                    print(f"Status {response.status} for user {user_id}, retrying...")
                    await asyncio.sleep(1)
                    return await self.check_user(session, user_id, yoinkers_dir, retry_count)
                    
        except asyncio.TimeoutError:
            if retry_count < 3:
                print(f"Timeout for user {user_id} (attempt {retry_count + 1}), retrying...")
                await asyncio.sleep(1)  # Brief delay before retry
                return await self.check_user(session, user_id, yoinkers_dir, retry_count + 1)
            else:
                print(f"Timeout for user {user_id} after 4 attempts, giving up")
                return None
        except aiohttp.ClientError as e:
            if retry_count < 3:
                print(f"Request error for user {user_id} (attempt {retry_count + 1}): {e}, retrying...")
                await asyncio.sleep(1)  # Brief delay before retry
                return await self.check_user(session, user_id, yoinkers_dir, retry_count + 1)
            else:
                print(f"Request error for user {user_id} after 4 attempts: {e}")
                return None
    
    async def process_user_ids(self, input_file: str, output_file: str, save_empty: bool = False):
        """Process user IDs from input file and write results to CSV"""
        
        if not os.path.exists(input_file):
            print(f"Error: Input file '{input_file}' not found!")
            return
        
        # Setup directories and files
        yoinkers_dir = "yoinkers"
        os.makedirs(yoinkers_dir, exist_ok=True)
        
        print(f"Reading user IDs from: {input_file}")
        print(f"Output will be written to: {output_file}")
        print(f"JSON responses will be saved to: {yoinkers_dir}/")
        print(f"Save empty results: {save_empty}")
        
        # Read and filter user IDs
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                user_ids = [line.strip() for line in f if line.strip()]
        except Exception as e:
            print(f"Error reading input file: {e}")
            return
        
        if not user_ids:
            print("No user IDs found in input file!")
            return
        
        print(f"Processing {len(user_ids)} user IDs...")
        
        # Load and filter 404 IDs
        print("Loading 404.txt file...")
        not_found_ids = self._load_404_ids()
        if not_found_ids:
            print(f"Loaded {len(not_found_ids)} IDs to skip from 404.txt")
        
        print("Filtering out known 404 IDs...")
        original_count = len(user_ids)
        user_ids = [uid for uid in user_ids if uid not in not_found_ids]
        skipped_count = original_count - len(user_ids)
        
        if skipped_count > 0:
            print(f"Skipped {skipped_count} IDs that previously returned 404")
        
        if not user_ids:
            print("All user IDs were previously processed (404.txt) or no valid IDs found!")
            return
        
        print(f"Found {len(user_ids)} user IDs to check")
        
        # Setup processing
        print("Setting up processing...")
        csv_lock = threading.Lock()
        csv_initialized = [False]
        
        # Process users with simpler approach
        print("Creating HTTP session...")
        connector = aiohttp.TCPConnector(limit=20, limit_per_host=3)
        timeout = aiohttp.ClientTimeout(total=10)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'YoinkerDetector Python Script'}
        ) as session:
            
            completed = 0
            found_count = 0
            
            # Process users in smaller concurrent batches
            batch_size = self.max_concurrent * 2  # Process 2x concurrent users at a time
            
            for i in range(0, len(user_ids), batch_size):
                batch = user_ids[i:i + batch_size]
                print(f"Processing batch {i//batch_size + 1}/{(len(user_ids) + batch_size - 1)//batch_size} ({len(batch)} users)")
                
                # Create tasks for this batch
                print(f"Creating tasks for batch...")
                tasks = []
                for user_id in batch:
                    task = asyncio.create_task(self.check_user(session, user_id, yoinkers_dir))
                    tasks.append((user_id, task))
                print(f"Created {len(tasks)} tasks, starting processing...")
                
                # Process results as they complete
                for i, (user_id, task) in enumerate(tasks):
                    try:
                        print(f"Waiting for task {i+1}/{len(tasks)}: {user_id}")
                        result = await asyncio.wait_for(task, timeout=5)
                        completed += 1
                        
                        # Save results
                        self._save_json_response(user_id, result, save_empty, yoinkers_dir)
                        self._append_csv_result(user_id, result, save_empty, output_file, csv_lock, csv_initialized)
                        
                        if result:
                            found_count += 1
                            print(f"[{completed}/{len(user_ids)}] FOUND: {result.get('userName', 'Unknown')} - {result.get('reason', 'Unknown reason')}")
                        else:
                            print(f"[{completed}/{len(user_ids)}] Not found: {user_id}")
                            
                    except asyncio.TimeoutError:
                        print(f"Timeout for user {user_id}")
                        completed += 1
                    except Exception as e:
                        print(f"Error processing user {user_id}: {e}")
                        completed += 1
        
        print(f"\nProcessing complete!")
        print(f"CSV results written to: {output_file}")
        print(f"JSON responses saved to: {yoinkers_dir}/")
        print(f"Total found: {found_count}")
        print(f"Total processed: {completed}")


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='Yoinker Detector - Check user IDs against yoinker detection service')
    parser.add_argument('input_file', help='Input file containing user IDs (one per line)')
    parser.add_argument('-o', '--output', default='yoinker_results.csv', 
                       help='Output CSV file (default: yoinker_results.csv)')
    parser.add_argument('-e', '--empty', action='store_true',
                       help='Save empty results (users not found) to CSV and JSON')
    parser.add_argument('-c', '--concurrent', type=int, default=5,
                       help='Maximum concurrent requests (default: 5)')
    
    args = parser.parse_args()
    
    detector = YoinkerDetector(max_concurrent=args.concurrent)
    await detector.process_user_ids(args.input_file, args.output, args.empty)


if __name__ == "__main__":
    asyncio.run(main())