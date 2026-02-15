
from processing_worker import process_ingestion_jobs
import os
import threading
import time

def run_worker():
    print("Starting standalone worker pulse...")
    # We can't easily run it "once" because it's a while True loop.
    # But we can run it in a thread and kill it after some time.
    t = threading.Thread(target=process_ingestion_jobs, daemon=True)
    t.start()
    
    # Wait for it to process one job
    print("Worker running for 10 seconds...")
    time.sleep(10)
    print("Worker pulse finished.")

if __name__ == "__main__":
    run_worker()
