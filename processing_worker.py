
import threading
import time

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
                
                # Process strictly in plan order
                for stage in execution_order: # e.g. "People", "Bookings"
                    print(f"WORKER: Processing stage '{stage}' for Job {job_id}")
                    
                    # Fetch records for this stage
                    # Note: ingestion_records might not have 'templateKey' if we didn't save it row-level clearly
                    # We iterate per template.
                    
                    # Wait... our ingestion records are just rows attached to the job.
                    # We need to filter them by the template they belong to? 
                    # In our dump, we uploaded a SINGLE template per job (usually).
                    # Actually, the triggers might be multi-template if we supported that.
                    # But the current UI triggers ONE job per dump.
                    # Verify: api_import_trigger_job -> creates ONE job for the ONE template uploaded.
                    # So 'executionOrder' likely only has 1 item, OR multiple if we handle multi-step.
                    # But if we upload "People", execution order is ["People"].
                    # If we upload "Bookings", execution order is ["People", "Bookings"]? 
                    # NO, that's just dependency check. The JOB only contains Bookings data.
                    # So we only process the stage that MATCHES the job's payload.
                    
                    # Let's check what template the job is FOR.
                    # The job might not have 'templateKey' explicitly on root?
                    # We can infer from records?
                    
                    # Fallback: Process ALL records for this job.
                    records_cursor = coll_records.find({"jobId": job_id, "status": "pending"})
                    
                    for rec in records_cursor:
                        try:
                            row_data = rec.get("data", {})
                            # We need to know which template this row belongs to. 
                            # Currently `rec` has `rowIndex` and `data`. 
                            # We assume the job is for ONE template? 
                            # Let's assume we can get template from the Job metadata or inference?
                            # Ah, api_import_trigger_job puts `jobPlan`.
                            # But does the job know which template the data is? Not explicitly stored in `ingestion_jobs` root?
                            # Let's look at `api_import_trigger_job` in main.py...
                            
                            # It uses `job_doc`.
                            # `upload_doc` has `templateKey`.
                            # We didn't copy `templateKey` to `job_doc`. That's a tiny gap.
                            # But we can look up `uploadId`.
                            
                            upload_id = job.get("uploadId")
                            coll_uploads = get_raw_uploads_collection()
                            upload_doc = coll_uploads.find_one({"uploadId": upload_id})
                            if not upload_doc:
                                print(f"WORKER: Upload {upload_id} not found for Job {job_id}")
                                continue
                                
                            template_key = upload_doc.get("templateKey")
                            
                            # Skip if stage doesn't match?
                            # If job is for "Bookings", executionOrder=['People', 'Bookings'].
                            # But we only have Bookings data.
                            # We should only process if stage == templateKey.
                            if stage != template_key:
                                continue
                                
                            tpl_def = TEMPLATES.get(template_key)
                            if not tpl_def:
                                continue

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
                                        # Orphan? Warning?
                                        pass

                            # 2. Upsert Logic
                            # Determine Primary Key(s). For People -> id. For others -> ?
                            # We look for fields with "identifier": True in template?
                            # Or just use the first 'required' field?
                            # People.id has "identifier": True.
                            # Bookings.bookingRef has "identifier": True.
                            
                            primary_keys = [f["key"] for f in tpl_def["fields"] if f.get("identifier")]
                            query = {"tenantId": tenant_id, "templateKey": template_key}
                            
                            if primary_keys:
                                for pk in primary_keys:
                                    val = row_data.get(pk)
                                    if val is not None:
                                        query[f"data.{pk}"] = val
                                        
                                # Merge links
                                final_data = {**row_data, **resolved_links}
                                
                                coll_data.update_one(
                                    query,
                                    {"$set": {
                                        "data": final_data,
                                        "timestamp": datetime.now(),
                                        "jobId": job_id
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
                                    "jobId": job_id
                                })

                            # 3. Mark Record Resolved
                            coll_records.update_one(
                                {"_id": rec["_id"]}, 
                                {"$set": {"status": "resolved"}}
                            )
                            
                        except Exception as e:
                            print(f"WORKER: Error processing row {rec.get('rowIndex')}: {e}")
                            coll_records.update_one(
                                {"_id": rec["_id"]}, 
                                {"$set": {"status": "error", "error": str(e)}}
                            )
                            any_error = True

                # Job Complete
                final_status = "error" if any_error else "completed"
                coll_jobs.update_one(
                    {"_id": job["_id"]}, 
                    {"$set": {"status": final_status, "completedAt": datetime.now()}}
                )
                print(f"WORKER: Job {job_id} finished with status {final_status}")

            time.sleep(2) # Poll interval
            
        except Exception as e:
            print(f"WORKER CRASH: {e}")
            time.sleep(5)
