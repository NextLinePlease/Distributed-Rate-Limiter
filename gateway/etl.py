import redis
import time
import csv
import os
from datetime import datetime

print("[ETL Worker] Booting up Extract-Transform-Load Pipeline...")

# Connect to Redis
redis_client = redis.Redis(host='redis_store', port=6379, decode_responses=True)
csv_file_path = "/app/logs/violations.csv"

# Ensure log directory exists
os.makedirs("/app/logs", exist_ok=True)

# Write CSV header if file doesn't exist
if not os.path.exists(csv_file_path):
    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Client_IP", "Violation_Timestamp", "Status"])

while True:
    try:
        # EXTRACT: Block and wait for data in the Redis queue
        item = redis_client.blpop("violation_queue", timeout=0)
        if item:
            raw_data = item[1]
            client_ip, unix_time = raw_data.split('|')
            
            # TRANSFORM: Convert raw timestamp to human-readable format
            clean_time = datetime.fromtimestamp(float(unix_time)).strftime('%Y-%m-%d %H:%M:%S')
            
            # LOAD: Append the structured data to our persistent CSV log
            with open(csv_file_path, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([client_ip, clean_time, "BLOCKED"])
                
            print(f"[ETL Worker] Processed and logged violation for {client_ip} at {clean_time}")
            
    except Exception as e:
        print(f"[ETL Worker] Error processing stream: {e}")
        time.sleep(5)