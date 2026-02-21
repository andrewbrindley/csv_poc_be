"""
webhook_manager.py

Handles firing outbound webhooks when ingestion jobs complete.

Each webhook is stored in MongoDB (webhooks collection) with:
  - url           : target endpoint
  - entityTypes   : list of template keys to filter on ([] = all)
  - active        : bool
  - oauth         : { enabled, tokenUrl, clientId, clientSecret, scope }

fire_webhooks() is called from processing_worker.py after a job finishes.
It runs each delivery in its own daemon thread so it never blocks the worker.
"""

import threading
import requests
from datetime import datetime, timezone


# --------------------------------------------------------------------------
# OAuth Token Fetching
# --------------------------------------------------------------------------

def _fetch_oauth_token(oauth_cfg: dict) -> str | None:
    """
    Fetch a Bearer token using OAuth 2.0 client_credentials grant.
    Returns the token string, or None on failure.
    """
    token_url = (oauth_cfg.get("tokenUrl") or "").strip()
    client_id = (oauth_cfg.get("clientId") or "").strip()
    client_secret = (oauth_cfg.get("clientSecret") or "").strip()
    scope = (oauth_cfg.get("scope") or "").strip()

    if not token_url or not client_id or not client_secret:
        print("WEBHOOK: OAuth enabled but tokenUrl/clientId/clientSecret missing — skipping token fetch")
        return None

    try:
        data = {
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        }
        if scope:
            data["scope"] = scope

        resp = requests.post(token_url, data=data, timeout=10)
        resp.raise_for_status()
        token = resp.json().get("access_token")
        if token:
            print(f"WEBHOOK: OAuth token fetched successfully from {token_url}")
        else:
            print(f"WEBHOOK: OAuth response had no access_token: {resp.text[:200]}")
        return token
    except Exception as e:
        print(f"WEBHOOK: OAuth token fetch failed ({token_url}): {e}")
        return None


# --------------------------------------------------------------------------
# Single Webhook Delivery
# --------------------------------------------------------------------------

def _deliver(webhook: dict, payload: dict) -> None:
    """
    Deliver one webhook.  Runs in a daemon thread.
    Fetches an OAuth token first if configured.
    """
    url = webhook.get("url", "").strip()
    name = webhook.get("name", url)
    if not url:
        return

    headers = {"Content-Type": "application/json"}

    oauth = webhook.get("oauth") or {}
    if oauth.get("enabled"):
        token = _fetch_oauth_token(oauth)
        if token:
            headers["Authorization"] = f"Bearer {token}"

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        print(f"WEBHOOK: Delivered '{name}' → {url} — HTTP {resp.status_code}")
    except Exception as e:
        print(f"WEBHOOK: Delivery failed '{name}' → {url}: {e}")


# --------------------------------------------------------------------------
# Public API
# --------------------------------------------------------------------------

def fire_webhooks(tenant_id: str, job: dict) -> None:
    """
    Query all active webhooks for this tenant, filter by entityTypes,
    and fire each one in a daemon thread.

    job is the full MongoDB job document (already including metrics,
    status, completedAt, jobPlan etc.).
    """
    try:
        from db_config import get_webhooks_collection
        coll = get_webhooks_collection()
        if coll is None:
            return

        webhooks = list(coll.find({"tenantId": tenant_id, "active": True}))
        if not webhooks:
            return

        # Determine which entity types this job touched
        execution_order = job.get("jobPlan", {}).get("executionOrder", [])

        # Build payload
        completed_at = job.get("completedAt")
        if isinstance(completed_at, datetime):
            completed_at_str = completed_at.isoformat() + "Z"
        elif completed_at:
            completed_at_str = str(completed_at)
        else:
            completed_at_str = datetime.now(timezone.utc).isoformat()

        payload = {
            "event": "job.completed",
            "jobId": job.get("jobId", ""),
            "tenantId": tenant_id,
            "status": job.get("status", "unknown"),
            "metrics": job.get("metrics", {}),
            "entityTypes": execution_order,
            "completedAt": completed_at_str,
        }

        for webhook in webhooks:
            entity_filter = webhook.get("entityTypes") or []

            # If filter is non-empty, only fire if there's an intersection
            if entity_filter:
                if not any(e in execution_order for e in entity_filter):
                    wh_name = webhook.get("name", webhook.get("url", ""))
                    print(f"WEBHOOK: Skipping '{wh_name}' — entity filter {entity_filter} not in job {execution_order}")
                    continue

            t = threading.Thread(target=_deliver, args=(webhook, payload), daemon=True)
            t.start()

    except Exception as e:
        print(f"WEBHOOK: fire_webhooks error: {e}")
