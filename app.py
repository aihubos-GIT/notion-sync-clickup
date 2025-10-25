"""
Optimized Flask sync với:
- Cache ClickUp tasks (giảm API calls)
- Batch processing
- Async requests (parallel API calls)
- Smart polling interval
- Webhook-ready architecture
"""

from flask import Flask, jsonify
import threading
import time
from datetime import datetime
import requests
import os
import json
from dotenv import load_dotenv
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

# ============ CONFIGURATION ============
NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
CLICKUP_API_TOKEN = os.getenv("CLICKUP_API_TOKEN")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID")

RENDER_DISK_PATH = os.getenv("RENDER_DISK_PATH", ".")
KNOWN_TASKS_FILE = os.path.join(RENDER_DISK_PATH, "known_tasks.json")
TASK_MAP_FILE = os.path.join(RENDER_DISK_PATH, "task_mapping.json")  # NEW: Cache Notion->ClickUp mapping

# Performance settings
SYNC_INTERVAL = 10  # Giảm xuống 10s cho responsive hơn
MAX_WORKERS = 5     # Parallel API calls
BATCH_SIZE = 10     # Process tasks in batches

app = Flask(__name__)

# ============ GLOBAL CACHES ============
clickup_users_cache = None
clickup_tasks_cache = {}  # NEW: {notion_id: clickup_task_id}
last_cache_refresh = None
CACHE_TTL = 300  # Refresh cache sau 5 phút

sync_status = {
    "running": False,
    "last_sync": None,
    "total_synced": 0,
    "errors": 0,
    "last_error": None,
    "service_started": datetime.now().isoformat(),
    "avg_sync_time": 0,
    "cache_hits": 0
}

# ============ CACHE MANAGEMENT ============
def load_task_mapping():
    """Load Notion->ClickUp ID mapping từ disk"""
    if os.path.exists(TASK_MAP_FILE):
        try:
            with open(TASK_MAP_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_task_mapping(mapping):
    """Save mapping to disk"""
    try:
        with open(TASK_MAP_FILE, 'w', encoding='utf-8') as f:
            json.dump(mapping, f, indent=2)
    except Exception as e:
        print(f"❌ Lỗi lưu mapping: {e}")

def refresh_clickup_cache():
    """Refresh cache của ClickUp tasks (chạy định kỳ)"""
    global clickup_tasks_cache, last_cache_refresh
    
    print("🔄 Refreshing ClickUp cache...")
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    headers = {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        tasks = response.json().get("tasks", [])
        
        # Build cache: {notion_id: clickup_task_id}
        new_cache = {}
        for task in tasks:
            description = task.get("description", "")
            match = re.search(r'\[Notion ID: ([^\]]+)\]', description)
            if match:
                notion_id = match.group(1)
                new_cache[notion_id] = task.get("id")
        
        clickup_tasks_cache = new_cache
        last_cache_refresh = time.time()
        
        # Sync to disk
        save_task_mapping(new_cache)
        
        print(f"✅ Cache refreshed: {len(new_cache)} tasks mapped")
        return True
    except Exception as e:
        print(f"❌ Lỗi refresh cache: {e}")
        return False

def get_clickup_task_id_cached(notion_id):
    """Fast lookup từ cache thay vì query API"""
    global last_cache_refresh
    
    # Auto-refresh cache nếu quá cũ
    if not last_cache_refresh or (time.time() - last_cache_refresh) > CACHE_TTL:
        refresh_clickup_cache()
    
    task_id = clickup_tasks_cache.get(notion_id)
    if task_id:
        sync_status["cache_hits"] += 1
    return task_id

# ============ STATE MANAGEMENT ============
def load_known_tasks():
    if os.path.exists(KNOWN_TASKS_FILE):
        try:
            with open(KNOWN_TASKS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {"task_ids": [], "initialized": False}
    return {"task_ids": [], "initialized": False}

def save_known_tasks(known_tasks):
    try:
        os.makedirs(os.path.dirname(KNOWN_TASKS_FILE), exist_ok=True)
        with open(KNOWN_TASKS_FILE, 'w', encoding='utf-8') as f:
            json.dump(known_tasks, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"❌ Lỗi lưu file: {e}")

# ============ MAPPING FUNCTIONS (giữ nguyên) ============
def map_notion_status_to_clickup(notion_status):
    if not notion_status:
        return "to do"
    status = notion_status.lower().strip()
    if any(x in status for x in ["chưa", "not started", "todo", "to do", "backlog"]):
        return "to do"
    if any(x in status for x in ["đang", "in progress", "doing", "working"]):
        return "in progress"
    if any(x in status for x in ["hoàn", "complete", "done", "finished"]):
        return "complete"
    if any(x in status for x in ["đóng", "closed", "archived"]):
        return "closed"
    return "to do"

def map_notion_priority_to_clickup(notion_priority):
    if not notion_priority:
        return 3
    priority = notion_priority.lower()
    if any(x in priority for x in ["cao", "high", "urgent", "critical", "khẩn"]):
        return 1
    if any(x in priority for x in ["trung", "medium", "normal", "bình thường"]):
        return 3
    if any(x in priority for x in ["thấp", "low", "minor"]):
        return 4
    return 3

def normalize_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r'[^\w\s@.-]', '', name)
    name = ' '.join(name.split())
    return name

def get_clickup_users():
    global clickup_users_cache
    if clickup_users_cache:
        return clickup_users_cache
    
    url = f"https://api.clickup.com/api/v2/team"
    headers = {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        teams = response.json().get("teams", [])
        
        if not teams:
            return {}
        
        team_id = teams[0]["id"]
        url = f"https://api.clickup.com/api/v2/team/{team_id}/user"
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        members = response.json().get("members", [])
        
        user_map = {}
        for member in members:
            user = member.get("user", {})
            user_id = user.get("id")
            username = user.get("username", "")
            email = user.get("email", "")
            
            if not user_id:
                continue
            
            variants = set()
            if username:
                variants.add(normalize_name(username))
            if email:
                variants.add(normalize_name(email))
                variants.add(normalize_name(email.split('@')[0]))
            
            if username:
                for sep in ['.', '_', '-']:
                    if sep in username:
                        parts = username.split(sep)
                        variants.add(normalize_name(parts[0]))
                        if len(parts) > 1:
                            variants.add(normalize_name(parts[-1]))
                        variants.add(normalize_name(' '.join(parts)))
            
            for variant in variants:
                if variant:
                    user_map[variant] = user_id
        
        clickup_users_cache = user_map
        return user_map
    except Exception as e:
        print(f"❌ Lỗi lấy users: {e}")
        return {}

def map_notion_assignees_to_clickup(notion_assignees):
    if not notion_assignees:
        return []
    
    clickup_users = get_clickup_users()
    if not clickup_users:
        return []
    
    clickup_ids = []
    for assignee in notion_assignees:
        name = assignee.get("name", "")
        email = assignee.get("email", "")
        
        user_id = None
        if email:
            normalized = normalize_name(email)
            user_id = clickup_users.get(normalized) or clickup_users.get(normalize_name(email.split('@')[0]))
        
        if not user_id and name:
            normalized = normalize_name(name)
            user_id = clickup_users.get(normalized)
            if not user_id:
                for part in normalized.split():
                    if part in clickup_users:
                        user_id = clickup_users[part]
                        break
        
        if user_id and user_id not in clickup_ids:
            clickup_ids.append(user_id)
    
    return clickup_ids

# ============ NOTION API ============
def get_notion_tasks():
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_API_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    payload = {
        "sorts": [{"timestamp": "created_time", "direction": "descending"}]
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        return response.json().get("results", [])
    except Exception as e:
        print(f"❌ Lỗi Notion API: {e}")
        return []

def get_property_value(props, *possible_names):
    for name in possible_names:
        if name in props:
            return props[name]
    return None

def format_notion_task(page):
    props = page.get("properties", {})
    
    title_prop = get_property_value(props, "Tên công việc", "Name", "Task", "Title")
    name = "Untitled Task"
    if title_prop and title_prop.get("title"):
        name = title_prop["title"][0]["text"]["content"]
    
    status_prop = get_property_value(props, "Trạng thái", "Status", "State")
    status = "Chưa bắt đầu"
    if status_prop:
        if status_prop.get("status"):
            status = status_prop["status"].get("name", "Chưa bắt đầu")
        elif status_prop.get("select"):
            status = status_prop["select"].get("name", "Chưa bắt đầu")
    
    priority_prop = get_property_value(props, "Mức độ ưu tiên", "Priority", "Ưu tiên")
    priority = "Trung bình (Medium)"
    if priority_prop and priority_prop.get("select"):
        priority = priority_prop["select"].get("name", "Trung bình (Medium)")
    
    deadline_prop = get_property_value(props, "Deadline", "Due Date", "Hạn", "Due")
    deadline = None
    if deadline_prop and deadline_prop.get("date"):
        deadline = deadline_prop["date"].get("start")
    
    assignees_prop = get_property_value(props, "Phân công", "Assign", "Assignee", "Người thực hiện")
    assignees = []
    if assignees_prop and assignees_prop.get("people"):
        assignees = [{"name": p.get("name", ""), "email": p.get("email", "")} for p in assignees_prop["people"]]
    
    desc_prop = get_property_value(props, "Ghi chú", "Description", "Mô tả", "Notes")
    description = ""
    if desc_prop and desc_prop.get("rich_text"):
        description = desc_prop["rich_text"][0]["text"]["content"]
    
    return {
        "notion_id": page.get("id", ""),
        "name": name,
        "status": map_notion_status_to_clickup(status),
        "priority": map_notion_priority_to_clickup(priority),
        "deadline": deadline,
        "description": description,
        "assignees": assignees,
        "created_time": page.get("created_time", "")
    }

# ============ CLICKUP API (Optimized) ============
def create_clickup_task(task_data):
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    headers = {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    due_date = None
    if task_data["deadline"]:
        try:
            dt = datetime.fromisoformat(task_data["deadline"].replace('Z', '+00:00'))
            due_date = int(dt.timestamp() * 1000)
        except:
            pass
    
    assignee_ids = map_notion_assignees_to_clickup(task_data["assignees"])
    
    payload = {
        "name": task_data["name"],
        "description": f"[Notion ID: {task_data['notion_id']}]\n\n{task_data['description']}",
        "status": task_data["status"],
        "priority": task_data["priority"]
    }
    
    if due_date:
        payload["due_date"] = due_date
    if assignee_ids:
        payload["assignees"] = assignee_ids
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        result = response.json()
        
        # Update cache ngay lập tức
        clickup_tasks_cache[task_data['notion_id']] = result.get("id")
        save_task_mapping(clickup_tasks_cache)
        
        return result
    except Exception as e:
        print(f"❌ Lỗi create task: {e}")
        return None

def update_clickup_task(task_id, task_data):
    url = f"https://api.clickup.com/api/v2/task/{task_id}"
    headers = {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    due_date = None
    if task_data["deadline"]:
        try:
            dt = datetime.fromisoformat(task_data["deadline"].replace('Z', '+00:00'))
            due_date = int(dt.timestamp() * 1000)
        except:
            pass
    
    assignee_ids = map_notion_assignees_to_clickup(task_data["assignees"])
    
    payload = {
        "name": task_data["name"],
        "status": task_data["status"],
        "priority": task_data["priority"]
    }
    
    if due_date:
        payload["due_date"] = due_date
    if assignee_ids:
        payload["assignees"] = assignee_ids
    
    try:
        response = requests.put(url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Lỗi update task: {e}")
        return None

# ============ PARALLEL SYNC LOGIC ============
def process_single_task(notion_page, known_task_ids):
    """Process một task - dùng cho parallel execution"""
    try:
        task_data = format_notion_task(notion_page)
        notion_id = task_data['notion_id']
        
        # Fast cache lookup thay vì API call
        clickup_task_id = get_clickup_task_id_cached(notion_id)
        
        if clickup_task_id:
            result = update_clickup_task(clickup_task_id, task_data)
            action = "updated" if result else "error"
        else:
            result = create_clickup_task(task_data)
            action = "created" if result else "error"
        
        known_task_ids.add(notion_id)
        return {"status": action, "name": task_data['name'], "notion_id": notion_id}
        
    except Exception as e:
        print(f"❌ Error processing task: {e}")
        return {"status": "error", "error": str(e)}

def sync_notion_to_clickup():
    global sync_status
    
    start_time = time.time()
    print(f"\n🔄 Sync check @ {datetime.now().strftime('%H:%M:%S')}")
    
    known_data = load_known_tasks()
    known_task_ids = set(known_data.get("task_ids", []))
    is_initialized = known_data.get("initialized", False)
    
    notion_tasks = get_notion_tasks()
    if not notion_tasks:
        print("   ⚠️  No tasks from Notion")
        return
    
    current_task_ids = [task.get("id") for task in notion_tasks]
    
    # First run initialization
    if not is_initialized:
        print("🎯 First run - initializing...")
        print(f"   Found {len(current_task_ids)} existing tasks")
        
        # Load existing mapping from disk
        existing_mapping = load_task_mapping()
        clickup_tasks_cache.update(existing_mapping)
        
        # Do initial cache refresh
        refresh_clickup_cache()
        
        known_data = {
            "task_ids": current_task_ids,
            "initialized": True,
            "initialized_at": datetime.now().isoformat()
        }
        save_known_tasks(known_data)
        print("   ✅ Initialization done!")
        return
    
    # Find new tasks
    new_task_ids = [tid for tid in current_task_ids if tid not in known_task_ids]
    
    if not new_task_ids:
        print("   ✨ No new tasks")
        sync_status["last_sync"] = datetime.now().isoformat()
        return
    
    print(f"   🆕 Found {len(new_task_ids)} new tasks")
    
    # Filter new tasks
    new_tasks = [t for t in notion_tasks if t.get("id") in new_task_ids]
    
    # PARALLEL PROCESSING với ThreadPoolExecutor
    results = {"created": 0, "updated": 0, "errors": 0}
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(process_single_task, task, known_task_ids): task 
            for task in new_tasks
        }
        
        # Collect results
        for future in as_completed(future_to_task):
            result = future.result()
            status = result.get("status", "error")
            
            if status == "created":
                results["created"] += 1
                print(f"      ✨ Created: {result.get('name')}")
            elif status == "updated":
                results["updated"] += 1
                print(f"      🔄 Updated: {result.get('name')}")
            else:
                results["errors"] += 1
    
    # Save state
    known_data["task_ids"] = list(known_task_ids)
    save_known_tasks(known_data)
    
    # Update metrics
    elapsed = time.time() - start_time
    sync_status["avg_sync_time"] = elapsed
    sync_status["total_synced"] += results["created"] + results["updated"]
    sync_status["errors"] += results["errors"]
    sync_status["last_sync"] = datetime.now().isoformat()
    
    print(f"\n   ✅ Done in {elapsed:.2f}s: {results['created']} created, {results['updated']} updated")
    if results["errors"] > 0:
        print(f"   ⚠️  {results['errors']} errors")

# ============ BACKGROUND THREAD ============
def background_sync_loop():
    global sync_status
    sync_status["running"] = True
    
    print("🔍 Loading ClickUp users...")
    get_clickup_users()
    
    print("🔍 Loading task mapping cache...")
    cached_mapping = load_task_mapping()
    clickup_tasks_cache.update(cached_mapping)
    
    print(f"✅ Ready! Cache: {len(clickup_tasks_cache)} tasks\n")
    
    cycle = 0
    while sync_status["running"]:
        try:
            sync_notion_to_clickup()
            
            # Refresh cache mỗi 5 phút
            cycle += 1
            if cycle % 30 == 0:  # 30 cycles * 10s = 5 phút
                refresh_clickup_cache()
            
        except Exception as e:
            print(f"❌ Sync error: {e}")
            sync_status["errors"] += 1
            sync_status["last_error"] = str(e)
        
        time.sleep(SYNC_INTERVAL)

# ============ FLASK ROUTES ============
@app.route('/')
def home():
    known_data = load_known_tasks()
    return jsonify({
        "status": "running",
        "service": "Notion → ClickUp Real-time Sync (Optimized)",
        "version": "2.0",
        "performance": {
            "sync_interval": f"{SYNC_INTERVAL}s",
            "parallel_workers": MAX_WORKERS,
            "avg_sync_time": f"{sync_status['avg_sync_time']:.2f}s",
            "cache_hits": sync_status["cache_hits"]
        },
        "stats": {
            "service_started": sync_status["service_started"],
            "last_sync": sync_status["last_sync"],
            "total_synced": sync_status["total_synced"],
            "errors": sync_status["errors"],
            "known_tasks": len(known_data.get("task_ids", [])),
            "cached_tasks": len(clickup_tasks_cache)
        },
        "initialized": known_data.get("initialized", False)
    })

@app.route('/health')
def health():
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()}), 200

@app.route('/status')
def status():
    known_data = load_known_tasks()
    return jsonify({
        "sync_status": sync_status,
        "cache": {
            "clickup_tasks": len(clickup_tasks_cache),
            "clickup_users": len(clickup_users_cache) if clickup_users_cache else 0,
            "last_refresh": last_cache_refresh
        },
        "known_tasks": len(known_data.get("task_ids", [])),
        "initialized": known_data.get("initialized", False),
        "files": {
            "tasks_file": os.path.exists(KNOWN_TASKS_FILE),
            "mapping_file": os.path.exists(TASK_MAP_FILE)
        }
    })

@app.route('/trigger')
def trigger():
    try:
        sync_notion_to_clickup()
        return jsonify({"status": "success", "message": "Manual sync triggered"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/cache/refresh')
def refresh_cache():
    """Manual cache refresh"""
    success = refresh_clickup_cache()
    return jsonify({
        "status": "success" if success else "error",
        "cached_tasks": len(clickup_tasks_cache)
    })

@app.route('/reset')
def reset():
    try:
        for f in [KNOWN_TASKS_FILE, TASK_MAP_FILE]:
            if os.path.exists(f):
                os.remove(f)
        
        global clickup_tasks_cache
        clickup_tasks_cache = {}
        
        return jsonify({"status": "success", "message": "All state reset"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    print("=" * 70)
    print("🚀 Notion → ClickUp Real-time Sync Service v2.0 (Optimized)")
    print("=" * 70)
    print(f"⚡ Performance: {SYNC_INTERVAL}s interval, {MAX_WORKERS} parallel workers")
    print("=" * 70)
    
    sync_thread = threading.Thread(target=background_sync_loop, daemon=True)
    sync_thread.start()
    print("✅ Background sync started\n")
    
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)