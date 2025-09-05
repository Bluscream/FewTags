#!/usr/bin/env python3
"""
UserTags Merger Script
Merges all JSON tag files into one comprehensive usertags.json file.
Handles different file formats and deduplicates users across files.
"""

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Any, Set
import argparse


class UserTagsMerger:
    def __init__(self, input_dir: str = ".", output_file: str = "usertags.json"):
        self.input_dir = Path(input_dir)
        self.output_file = Path(output_file)
        self.merged_data = {}
        self.user_ids_seen: Set[str] = set()
        self.stats = {
            "files_processed": 0,
            "total_records": 0,
            "unique_users": 0,
            "duplicates_merged": 0
        }

    def load_json_file(self, file_path: Path) -> Dict[str, Any]:
        """Load and parse a JSON file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            print(f"Error parsing {file_path}: {e}")
            return {}
        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return {}

    def normalize_user_id(self, record: Dict[str, Any]) -> str:
        """Extract and normalize UserID from a record."""
        # Handle different field names for UserID
        user_id = record.get('UserID') or record.get('UserId')
        if not user_id:
            return None
        return str(user_id).strip()

    def extract_tags_from_record(self, record: Dict[str, Any], source_file: str) -> List[Dict[str, str]]:
        """Extract all tags/text from a record, handling different formats."""
        tags = []
        
        # Handle Tag array format (FewTags.json, ExternalTags.json, FewTags2.json)
        if 'Tag' in record and isinstance(record['Tag'], list):
            tags.extend(record['Tag'])
        
        # Handle NamePlatesText array format (FewTags-CVR.json)
        if 'NamePlatesText' in record and isinstance(record['NamePlatesText'], list):
            tags.extend(record['NamePlatesText'])
        
        # Handle BigPlatesText array format (FewTags-CVR.json)
        if 'BigPlatesText' in record and isinstance(record['BigPlatesText'], list):
            tags.extend(record['BigPlatesText'])
        
        # Handle individual text fields (FewTagsv2.json)
        text_fields = ['PlateText', 'PlateText2', 'PlateText3', 'PlateBigText']
        for field in text_fields:
            if field in record and record[field]:
                tags.append(record[field])
        
        # Handle single Text field (Nameplates.json, NamePlatesv2.json)
        if 'Text' in record and record['Text']:
            tags.append(record['Text'])
        
        # Filter out empty tags and return with source information
        filtered_tags = []
        for tag in tags:
            if tag and str(tag).strip():
                filtered_tags.append({
                    'text': str(tag).strip(),
                    'source': source_file
                })
        
        return filtered_tags

    def extract_foreground_color(self, record: Dict[str, Any]) -> str:
        """Extract foreground color from Color array or other fields."""
        if 'Color' in record and isinstance(record['Color'], list) and len(record['Color']) >= 3:
            # Convert RGB values to hex
            r, g, b = record['Color'][:3]
            return f"#{r:02x}{g:02x}{b:02x}"
        return "#ff0000"  # Default red color

    def extract_main_tag(self, tags: List[str]) -> str:
        """Extract a main tag from the list of tags (first non-empty tag, cleaned)."""
        for tag in tags:
            if tag and str(tag).strip():
                # Remove HTML tags and color codes for a clean main tag
                import re
                clean_tag = re.sub(r'<[^>]+>', '', tag)
                clean_tag = clean_tag.strip()
                if clean_tag:
                    return clean_tag
        return "User"

    def merge_record(self, record: Dict[str, Any], source_file: str) -> None:
        """Merge a single record into the merged data."""
        user_id = self.normalize_user_id(record)
        if not user_id:
            return
        
        tag_objects = self.extract_tags_from_record(record, source_file)
        if not tag_objects:
            return
        
        # Check if we've seen this user before
        if user_id in self.user_ids_seen:
            # Merge tags, avoiding duplicates based on text content
            existing_tags = self.merged_data[user_id].get('tags', [])
            existing_tag_texts = set(existing_tags)
            
            for tag_obj in tag_objects:
                tag_text = tag_obj['text']
                if tag_text not in existing_tag_texts:
                    existing_tags.append(tag_text)
                    existing_tag_texts.add(tag_text)
            
            # Add source to sources list if not already present
            sources = self.merged_data[user_id].get('sources', [])
            if source_file not in sources:
                sources.append(source_file)
            self.merged_data[user_id]['sources'] = sources
            
            self.merged_data[user_id]['tags'] = existing_tags
            self.stats["duplicates_merged"] += 1
        else:
            # Create new record with UserID as key
            tag_texts = [tag_obj['text'] for tag_obj in tag_objects]
            
            new_record = {
                "id": record.get('id', 0),
                "active": record.get('Active', True),
                "malicious": record.get('Malicious', False),
                "tags": tag_texts,
                "tag": self.extract_main_tag(tag_texts),
                "foreground_color": self.extract_foreground_color(record),
                "sources": [source_file]
            }
            
            self.merged_data[user_id] = new_record
            self.user_ids_seen.add(user_id)
            self.stats["unique_users"] += 1
        
        self.stats["total_records"] += 1

    def process_file(self, file_path: Path) -> None:
        """Process a single JSON file."""
        print(f"Processing {file_path.name}...")
        
        data = self.load_json_file(file_path)
        if not data or 'records' not in data:
            print(f"  No valid records found in {file_path.name}")
            return
        
        records = data['records']
        print(f"  Found {len(records)} records")
        
        for record in records:
            self.merge_record(record, file_path.name)
        
        self.stats["files_processed"] += 1

    def merge_all_files(self) -> None:
        """Merge all JSON files in the input directory."""
        json_files = list(self.input_dir.glob("*.json"))
        
        if not json_files:
            print(f"No JSON files found in {self.input_dir}")
            return
        
        print(f"Found {len(json_files)} JSON files to process:")
        for file_path in json_files:
            print(f"  - {file_path.name}")
        
        print("\nStarting merge process...")
        
        # Exclude generated files and the output file
        excluded_files = {self.output_file.name, 'usertags.json', 'usertags2.json', 'usertags_new.json', 'usertags_final.json'}
        
        for file_path in json_files:
            if file_path.name not in excluded_files:
                self.process_file(file_path)
        
        print(f"\nMerge completed!")
        print(f"Files processed: {self.stats['files_processed']}")
        print(f"Total records processed: {self.stats['total_records']}")
        print(f"Unique users: {self.stats['unique_users']}")
        print(f"Duplicates merged: {self.stats['duplicates_merged']}")

    def save_merged_data(self) -> None:
        """Save the merged data to the output file."""
        try:
            with open(self.output_file, 'w', encoding='utf-8') as f:
                json.dump(self.merged_data, f, indent=2, ensure_ascii=False)
            print(f"\nMerged data saved to {self.output_file}")
            print(f"Total records in output: {len(self.merged_data)}")
        except Exception as e:
            print(f"Error saving merged data: {e}")

    def run(self) -> None:
        """Run the complete merge process."""
        print("UserTags Merger")
        print("=" * 50)
        
        if not self.input_dir.exists():
            print(f"Input directory {self.input_dir} does not exist!")
            return
        
        self.merge_all_files()
        self.save_merged_data()


def main():
    parser = argparse.ArgumentParser(description="Merge JSON tag files into one comprehensive usertags.json")
    parser.add_argument("--input-dir", "-i", default=".", 
                       help="Input directory containing JSON files (default: current directory)")
    parser.add_argument("--output", "-o", default="usertags.json",
                       help="Output file name (default: usertags.json)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Show what would be processed without actually merging")
    
    args = parser.parse_args()
    
    merger = UserTagsMerger(args.input_dir, args.output)
    
    if args.dry_run:
        json_files = list(Path(args.input_dir).glob("*.json"))
        print(f"Would process {len(json_files)} JSON files:")
        for file_path in json_files:
            if file_path.name != args.output:
                print(f"  - {file_path.name}")
        return
    
    merger.run()


if __name__ == "__main__":
    main()
