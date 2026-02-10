import os
import random
import datetime
import argparse

# Set the base path for file generation (corresponding to docker-compose volumes)
# Docker container path: /tmp/test (mapped from ./data/test)
BASE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'test')

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def generate_record(date_str, invalid_size=False, empty_data=False, duplicate_ids=False):
    filename = f"record_{date_str}.txt"
    filepath = os.path.join(BASE_DIR, filename)
    
    ensure_dir(BASE_DIR)
    
    print(f"Generating {filename}...")

    if invalid_size:
        # Case 1: File size <= 50 bytes
        content = "id,name,score\n1,A,10" # Very short content
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  [Invalid] Created small file ({len(content)} bytes).")
        return

    if empty_data:
        # Case 2: Only header
        content = "id,name,score,date,"
        # fill header to make it > 50 bytes but still empty data
        content += "#" * 60 + "\n" 
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"  [Invalid] Created empty data file (only header).")
        return

    # Generate valid data first
    records = []
    base_id = 1000
    for i in range(10):
        rec_id = base_id + i
        name = f"User{chr(65+i)}"
        score = random.randint(0, 100)
        records.append(f"{rec_id},{name},{score},{date_str}")
    
    if duplicate_ids:
        # Case 3: Duplicate IDs
        records.append(f"{base_id},{'DuplicateUser'},{0},{date_str}")
        print(f"  [Invalid] Added duplicate ID: {base_id}")

    # Write file
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write("id,name,score,date\n")
        f.write("\n".join(records))
    
    print(f"  [Success] Created file at {filepath}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate fake data for Airflow DAG testing.")
    parser.add_argument("--date", type=str, help="Date in YYYYMMDD format", default=datetime.datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--type", choices=['valid', 'small', 'empty', 'duplicate'], default='valid', help="Type of file to generate")
    
    args = parser.parse_args()
    
    invalid_size = (args.type == 'small')
    empty_data = (args.type == 'empty')
    duplicate_ids = (args.type == 'duplicate')
        
    generate_record(args.date, invalid_size, empty_data, duplicate_ids)
