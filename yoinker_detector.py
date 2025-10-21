#!/usr/bin/env python3
"""
Yoinker Detector - Simplified and Optimized
Checks user IDs against the yoinker detection service with 404 tracking
"""

import hashlib
import json
import csv
import asyncio
import aiohttp
from datetime import datetime, timedelta, UTC
from typing import Optional, Dict, Set, List
import argparse
import os
from collections import deque


class YoinkerDetector:
    """Simplified yoinker detector with optimized performance"""
    
    def __init__(self, max_concurrent: int = 10):
        self.max_concurrent = max_concurrent
        self.base_url = "https://yd.just-h.party/"
        self.rate_limit_queue = deque()
        self.cache: Dict[str, Dict] = {}
        self.request_count = 0
        self.max_requests_per_minute = 15
    
    def _generate_hash(self, user_id: str) -> str:
        """Generate SHA256 hash of user ID"""
        return hashlib.sha256(user_id.encode('utf-8')).hexdigest()
    
    def _check_rate_limit(self) -> bool:
        """Simple rate limiting: 15 requests per 60 seconds"""
        now = datetime.now(UTC)
        # Remove old requests (older than 60 seconds)
        while self.rate_limit_queue and (now - self.rate_limit_queue[0]).total_seconds() > 60:
            self.rate_limit_queue.popleft()
        
        return len(self.rate_limit_queue) < self.max_requests_per_minute
    
    def _record_request(self):
        """Record a request for rate limiting"""
        self.rate_limit_queue.append(datetime.now(UTC))
    
    def _load_404s(self) -> Set[str]:
        """Load existing 404 user IDs"""
        not_found_file = "404.txt"
        if not os.path.exists(not_found_file):
            return set()
        
        try:
            with open(not_found_file, 'r', encoding='utf-8') as f:
                return {line.strip() for line in f if line.strip()}
        except Exception as e:
            print(f"Error reading {not_found_file}: {e}")
            return set()
    
    def _save_404s(self, new_404s: Set[str]):
        """Save new 404 user IDs"""
        if not new_404s:
            return
        
        not_found_file = "404.txt"
        try:
            with open(not_found_file, 'a', encoding='utf-8') as f:
                for user_id in new_404s:
                    f.write(f"{user_id}\n")
            print(f"Added {len(new_404s)} new 404s to {not_found_file}")
        except Exception as e:
            print(f"Error saving 404s: {e}")
    
    async def _check_user(self, session: aiohttp.ClientSession, user_id: str) -> Optional[Dict]:
        """Check a single user ID"""
        # Check cache first
        if user_id in self.cache:
            return self.cache[user_id]
        
        # Check for existing JSON file
        json_file = f"yoinkers/{user_id}.json"
        if os.path.exists(json_file):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.cache[user_id] = data
                    return data
            except:
                pass
        
        # Rate limiting
        while not self._check_rate_limit():
            await asyncio.sleep(1)
        
        user_hash = self._generate_hash(user_id)
        url = f"{self.base_url}{user_hash}"
        
        try:
            async with session.get(url, timeout=30) as response:
                self._record_request()
                
                if response.status == 404:
                    result = {"_404": True, "userId": user_id}
                    self.cache[user_id] = result
                    return result
                
                elif response.status == 200:
                    data = await response.json()
                    if data.get('isYoinker', False):
                        self.cache[user_id] = data
                        return data
                    else:
                        self.cache[user_id] = None
                        return None
                
                elif response.status == 429:
                    print(f"Rate limited, waiting...")
                    await asyncio.sleep(60)
                    return await self._check_user(session, user_id)
                
                else:
                    print(f"HTTP {response.status} for {user_id}")
                    return None
                    
        except Exception as e:
            print(f"Error checking {user_id}: {e}")
            return None
    
    def _save_json(self, user_id: str, data: Optional[Dict], save_empty: bool):
        """Save JSON response if needed"""
        if not data or data.get('_404'):
            return
        
        os.makedirs("yoinkers", exist_ok=True)
        json_file = f"yoinkers/{user_id}.json"
        
        try:
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"Error saving JSON for {user_id}: {e}")
    
    async def process_user_ids(self, input_file: str, output_file: str, save_empty: bool = False):
        """Main processing function"""
        if not os.path.exists(input_file):
            print(f"Input file not found: {input_file}")
            return
        
        # Load existing 404s
        known_404s = self._load_404s()
        if known_404s:
            print(f"Loaded {len(known_404s)} known 404s")
        
        # Read and filter user IDs
        with open(input_file, 'r', encoding='utf-8') as f:
            all_user_ids = [line.strip() for line in f if line.strip()]
        
        user_ids = [uid for uid in all_user_ids if uid not in known_404s]
        skipped = len(all_user_ids) - len(user_ids)
        
        if skipped:
            print(f"Skipped {skipped} known 404s")
        print(f"Processing {len(user_ids)} user IDs")
        
        # Track results
        results = []
        new_404s = set()
        semaphore = asyncio.Semaphore(self.max_concurrent)
        
        async def process_user(user_id: str):
            async with semaphore:
                return await self._check_user(session, user_id)
        
        # Process users
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=20)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={'User-Agent': 'YoinkerDetector'}
        ) as session:
            
            tasks = [process_user(uid) for uid in user_ids]
            
            for i, coro in enumerate(asyncio.as_completed(tasks)):
                result = await coro
                user_id = user_ids[i]
                
                if result and result.get('_404'):
                    new_404s.add(user_id)
                    print(f"[{i+1}/{len(user_ids)}] 404: {user_id}")
                elif result:
                    self._save_json(user_id, result, save_empty)
                    results.append({
                        'user_id': result.get('userId', user_id),
                        'user_name': result.get('userName', ''),
                        'year': result.get('year', ''),
                        'reason': result.get('reason', '')
                    })
                    print(f"[{i+1}/{len(user_ids)}] FOUND: {result.get('userName', 'Unknown')}")
                else:
                    if save_empty:
                        results.append({
                            'user_id': user_id,
                            'user_name': '',
                            'year': '',
                            'reason': 'Not found'
                        })
                    print(f"[{i+1}/{len(user_ids)}] Not found: {user_id}")
        
        # Save results
        self._save_404s(new_404s)
        
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(['UserId', 'UserName', 'Year', 'Reason'])
            for result in results:
                writer.writerow([
                    result['user_id'],
                    result['user_name'], 
                    result['year'],
                    result['reason']
                ])
        
        print(f"\nComplete! Found {len(results)} results")
        if new_404s:
            print(f"New 404s: {len(new_404s)}")


async def main():
    parser = argparse.ArgumentParser(description='Yoinker Detector')
    parser.add_argument('input_file', help='Input file with user IDs')
    parser.add_argument('-o', '--output', default='yoinker_results.csv', help='Output CSV file')
    parser.add_argument('-e', '--empty', action='store_true', help='Save empty results')
    parser.add_argument('-c', '--concurrent', type=int, default=10, help='Max concurrent requests')
    
    args = parser.parse_args()
    
    detector = YoinkerDetector(max_concurrent=args.concurrent)
    await detector.process_user_ids(args.input_file, args.output, args.empty)


if __name__ == "__main__":
    asyncio.run(main())