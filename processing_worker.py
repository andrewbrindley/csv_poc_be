

import threading
import time
from datetime import datetime
from db_config import (
    get_ingestion_jobs_collection,
    get_ingestion_records_collection,
    get_tenant_data_collection,
    get_raw_uploads_collection
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from template_utils import get_templates

# --------------------------------------------------------------------------
# Helper: Process Single Record (Thread-Safe)
# --------------------------------------------------------------------------
def process_single_record(rec, tenant_id, template_key, job_id, tpl_def):
    """
    Process a single record: resolve refs -> upsert -> update status.
    Returns True if error occurred, False otherwise.
    """
    try:
        coll_data = get_tenant_data_collection()
        coll_records = get_ingestion_records_collection()
        
        # Data is PRE-CLEANED by frontend (validated, edited by user)
        # Worker only needs to: 1) Resolve references, 2) Upsert
        row_data = rec.get("data", {})

        # 1. Resolve References
        refs = tpl_def.get("references", {})
        resolved_links = {}
        
        for field, ref_config in refs.items():
            target_tpl = ref_config["targetTemplate"]
            target_field = ref_config["targetField"]
            source_val = row_data.get(field)
            
            if source_val:
                # Look up parent
                # "data.id": "1001", "templateKey": "People", "tenantId": tenant_id
                parent = coll_data.find_one({
                    "tenantId": tenant_id,
                    "templateKey": target_tpl,
                    f"data.{target_field}": source_val
                })
                
                if parent:
                    resolved_links[f"_parentRef_{field}"] = {
                        "collection": target_tpl,
                        "id": str(parent["_id"]),
                        "label": f"{target_tpl} {source_val}" 
                    }
                else:
                    # Strict FK Check: Parent must exist
                    raise ValueError(f"Foreign Key Violation: Field '{field}' value '{source_val}' not found in {target_tpl}")

        # 2. Upsert Logic
        # Extract all fields marked with "identifier": True from template.
        # Supports both single and composite keys:
        #   - People: single key (id)
        #   - Bookings: single key (bookingRef)
        #   - PatientData: composite key (patientId + assessmentName)
        # All identifier fields are combined in the query for uniqueness.
        
        primary_keys = [f["key"] for f in tpl_def["fields"] if f.get("identifier")]
        query = {"tenantId": tenant_id, "templateKey": template_key}
        
        if primary_keys:
            # Add all identifier fields to query (composite key support)
            for pk in primary_keys:
                val = row_data.get(pk)
                if val is not None:
                    query[f"data.{pk}"] = val
                    
            # Merge links
            final_data = {**row_data, **resolved_links}
            
            # Check if record exists to determine operation type
            # This prevents race condition from separate update calls
            exists = coll_data.find_one(query) is not None
            operation = "updated" if exists else "created"
            # print(f"WORKER: Upsert Logic - Query: {query}, Exists: {exists} -> Operation: {operation}")
            
            # Perform upsert with operation tracking in single atomic call
            coll_data.update_one(
                query,
                {"$set": {
                    "data": final_data,
                    "timestamp": datetime.now(),
                    "jobId": job_id,
                    "__operation": operation
                }},
                upsert=True
            )
        else:
            # No identifier? persistent append?
            final_data = {**row_data, **resolved_links}
            coll_data.insert_one({
                "tenantId": tenant_id,
                "templateKey": template_key,
                "data": final_data,
                "timestamp": datetime.now(),
                "jobId": job_id,
                "__operation": "created"
            })

        # 3. Mark Record Resolved with SNAPSHOT
        coll_records.update_one(
            {"_id": rec["_id"]}, 
            {"$set": {
                "status": "resolved",
                "processedData": {
                    **final_data,
                    "__operation": operation
                }
            }, "$unset": {"error": ""}}
        )
        return {"status": "success", "operation": operation}

    except Exception as e:
        print(f"WORKER: Error processing row {rec.get('rowIndex')}: {e}")
        # Re-acquire collection just in case (though should be safe)
        coll_records = get_ingestion_records_collection()
        coll_records.update_one(
            {"_id": rec["_id"]}, 
            {"$set": {"status": "error", "error": str(e)}}
        )
        return {"status": "error", "error": str(e)}

# --------------------------------------------------------------------------
# Background Worker: Job Processing
# --------------------------------------------------------------------------
def process_ingestion_jobs():
    """
    Background worker that polls for pending jobs and processes them.
    Logic:
      1. Find pending jobs (sorted by creation).
      2. Check dependencies (if job A depends on B, B must be completed).
      3. If runnable, switch status to 'processing'.
      4. Iterate records:
         - Resolve references (look up parent in tenant_data).
         - Upsert into tenant_data (using primary keys).
         - Update record status to 'resolved'.
      5. Update job status to 'completed'.
    """
    print("WORKER: Ingestion processing thread started.")
    
    while True:
        try:
            coll_jobs = get_ingestion_jobs_collection()
            coll_records = get_ingestion_records_collection()
            coll_data = get_tenant_data_collection()

            if coll_jobs is None or coll_records is None or coll_data is None:
                time.sleep(5)
                continue

            # 1. Find pending jobs
            # Sort by triggeredAt asc to process oldest first
            pending_jobs = list(coll_jobs.find({"status": "pending"}).sort("triggeredAt", 1))

            for job in pending_jobs:
                job_id = job["jobId"]
                tenant_id = job["tenantId"]
                job_plan = job.get("jobPlan", {})
                execution_order = job_plan.get("executionOrder", [])
                
                # Simple serialized processing for now: verify dependencies are met
                # Ideally we check against DB for dependency job completion, 
                # but here we just assume sequential processing of the queue handles it 
                # if we process roughly in order. 
                # For strict DAG correctness, we'd check if dependent types are populated.
                
                print(f"WORKER: Starting Job {job_id}...")
                
                # Mark as processing
                coll_jobs.update_one({"_id": job["_id"]}, {"$set": {"status": "processing"}})
                
                any_error = False
                
                
                # PRE-INIT Stats to accumulate across stages
                aggregate_stats = {"created": 0, "updated": 0, "errors": 0}
                
                # Process strictly in plan order
                for stage in execution_order: # e.g. "People", "Bookings"
                    print(f"WORKER: Processing stage '{stage}' for Job {job_id}")
                    
                    # FETCH UPLOAD ID FROM STAGE CONFIG
                    # This supports multi-template jobs where each stage might come from a different upload
                    stage_config = job_plan.get("stages", {}).get(stage)
                    if not stage_config:
                        print(f"WORKER: No configuration found for stage '{stage}'")
                        continue

                    upload_id = stage_config.get("uploadId")
                    print(f"WORKER: Stage '{stage}' upload_id: '{upload_id}' (type: {type(upload_id)})")
                    
                    if not upload_id:
                        print(f"WORKER: ERROR - No Upload ID for stage '{stage}' in Job {job_id}. Skipping.")
                        continue

                    # OPTIMIZATION: Fetch upload document ONCE before processing records
                    coll_uploads = get_raw_uploads_collection()
                    upload_doc = coll_uploads.find_one({"uploadId": upload_id})
                    
                    if not upload_doc:
                        print(f"WORKER: Upload doc NOT FOUND for '{upload_id}' in Job {job_id}")
                        continue
                    
                    template_key = upload_doc.get("templateKey")
                    
                    # Validation: Stage MUST match Template Key (sanity check)
                    if stage != template_key:
                        print(f"WORKER: Mismatch! Stage '{stage}' != Upload Template '{template_key}'")
                        continue
                    
                    all_templates = get_templates(tenant_id)
                    tpl_def = all_templates.get(template_key)
                    if not tpl_def:
                        print(f"WORKER: Template {template_key} not found")
                        continue
                    
                    # Now process all records for this stage
                    # CRITICAL FIX: Query by BOTH jobId AND templateKey
                    records_cursor = coll_records.find({
                        "jobId": job_id, 
                        "templateKey": template_key,
                        "status": "pending"
                    })
                    pending_records = list(records_cursor)
                    
                    if not pending_records:
                        print(f"WORKER: No pending records found for stage '{stage}'")
                        continue

                    print(f"WORKER: Processing {len(pending_records)} '{stage}' records with 5 threads...")

                    # Parallel Processing
                    stage_stats = {"created": 0, "updated": 0, "errors": 0}
                    
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        futures = [
                            executor.submit(process_single_record, rec, tenant_id, template_key, job_id, tpl_def)
                            for rec in pending_records
                        ]
                        
                        for future in as_completed(futures):
                            res = future.result()
                            if res["status"] == "error":
                                stage_stats["errors"] += 1
                                any_error = True
                            elif res["status"] == "success":
                                op = res.get("operation", "updated")
                                if op == "created":
                                    stage_stats["created"] += 1
                                else:
                                    stage_stats["updated"] += 1
                    
                    # Accumulate into total stats
                    aggregate_stats["created"] += stage_stats["created"]
                    aggregate_stats["updated"] += stage_stats["updated"]
                    aggregate_stats["errors"] += stage_stats["errors"]
                    print(f"WORKER: Stage '{stage}' complete. Stats: {stage_stats}")

                # Job Complete
                final_status = "error" if any_error else "completed"
                coll_jobs.update_one(
                    {"_id": job["_id"]}, 
                    {
                        "$set": {
                            "status": final_status, 
                            "completedAt": datetime.now(),
                            "metrics": {
                                "totalRecords": aggregate_stats["created"] + aggregate_stats["updated"] + aggregate_stats["errors"],
                                "created": aggregate_stats["created"],
                                "updated": aggregate_stats["updated"],
                                "errors": aggregate_stats["errors"]
                            }
                        }
                    }
                )
                print(f"WORKER: Job {job_id} finished with status {final_status}. Aggregate Stats: {aggregate_stats}")

            time.sleep(2) # Poll interval
            
        except Exception as e:
            print(f"WORKER CRASH: {e}")
            time.sleep(5)
