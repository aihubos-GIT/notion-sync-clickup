"""
Flask wrapper để chạy sync script như Web Service trên Render
Optimized: Sync tối ưu các cột, map assignees thông minh hơn
REFACTORED v1.3: Unified logging với automation_log.json
"""

from flask import Flask, jsonify, request
import threading
import time
from datetime import datetime
import requests
import os
import json
from dotenv import load_dotenv
import re
from logger_utils import logger

# Load environment variables
load_dotenv()

# ============ CONFIGURATION ============
NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
CLICKUP_API_TOKEN = os.getenv("CLICKUP_API_TOKEN")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID")

RENDER_DISK_PATH = os.getenv("RENDER_DISK_PATH", ".")
KNOWN_TASKS_FILE = os.path.join(RENDER_DISK_PATH, "known_tasks.json")

logger.info("system", "config_loaded", f"Data path: {KNOWN_TASKS_FILE}")

app = Flask(__name__)

# Global state
sync_status = {
    "running": False,
    "last_sync": None,
    "total_synced": 0,
    "errors": 0,
    "last_error": None,
    "service_started": datetime.now().isoformat()
}

# ============ STATE MANAGEMENT ============
def load_known_tasks():
    if os.path.exists(KNOWN_TASKS_FILE):
        try:
            with open(KNOWN_TASKS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                logger.info(
                    "notion_sync",
                    "state_loaded",
                    f"Loaded {len(data.get('task_ids', []))} tasks from state file",
                    extra={"initialized": data.get('initialized', False)}
                )
                return data
        except Exception as e:
            logger.error("notion_sync", "state_load_error", f"Error reading state file: {e}")
            return {"task_ids": [], "initialized": False}
    
    logger.info("notion_sync", "state_init", "State file not found, creating new")
    return {"task_ids": [], "initialized": False}

def save_known_tasks(known_tasks):
    try:
        os.makedirs(os.path.dirname(KNOWN_TASKS_FILE), exist_ok=True)
        with open(KNOWN_TASKS_FILE, 'w', encoding='utf-8') as f:
            json.dump(known_tasks, f, indent=2, ensure_ascii=False)
        logger.info(
            "notion_sync",
            "state_saved",
            f"Saved state: {len(known_tasks.get('task_ids', []))} tasks"
        )
    except Exception as e:
        logger.error("notion_sync", "state_save_error", f"Error saving state file: {e}")

# ============ STATUS & PRIORITY MAPPING ============
def map_notion_status_to_clickup(notion_status):
    """Map status từ Notion sang ClickUp với nhiều variants"""
    if not notion_status:
        return "to do"
    
    status = notion_status.lower().strip()
    
    # To Do variants
    if any(x in status for x in ["chưa", "not started", "todo", "to do", "backlog"]):
        return "to do"
    
    # In Progress variants
    if any(x in status for x in ["đang", "in progress", "doing", "working"]):
        return "in progress"
    
    # Complete variants
    if any(x in status for x in ["hoàn", "complete", "done", "finished"]):
        return "complete"
    
    # Closed variants
    if any(x in status for x in ["đóng", "closed", "archived"]):
        return "closed"
    
    return "to do"

def map_notion_priority_to_clickup(notion_priority):
    """Map priority từ Notion sang ClickUp - càng nhỏ càng ưu tiên cao"""
    if not notion_priority:
        return 3
    
    priority = notion_priority.lower()
    
    # Urgent/High = 1
    if any(x in priority for x in ["cao", "high", "urgent", "critical", "khẩn"]):
        return 1
    
    # Normal/Medium = 3
    if any(x in priority for x in ["trung", "medium", "normal", "bình thường"]):
        return 3
    
    # Low = 4
    if any(x in priority for x in ["thấp", "low", "minor"]):
        return 4
    
    return 3

# ============ CLICKUP USER MANAGEMENT (OPTIMIZED) ============
clickup_users_cache = None

def normalize_name(name):
    """Chuẩn hóa tên để so sánh: lowercase, bỏ dấu, bỏ khoảng trắng thừa"""
    if not name:
        return ""
    
    name = name.lower().strip()
    # Bỏ các ký tự đặc biệt
    name = re.sub(r'[^\w\s@.-]', '', name)
    # Chuẩn hóa khoảng trắng
    name = ' '.join(name.split())
    return name

def get_clickup_users():
    """Cache danh sách users từ ClickUp với nhiều key để match dễ hơn"""
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
            logger.warning("notion_sync", "no_teams", "No ClickUp teams found")
            return {}
        
        team_id = teams[0]["id"]
        url = f"https://api.clickup.com/api/v2/team/{team_id}/user"
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        members = response.json().get("members", [])
        
        user_map = {}
        
        logger.info("notion_sync", "users_loading", f"Found {len(members)} ClickUp users")
        
        for member in members:
            user = member.get("user", {})
            user_id = user.get("id")
            username = user.get("username", "")
            email = user.get("email", "")
            
            if not user_id:
                continue
            
            # Lưu nhiều variants của tên để dễ match
            variants = set()
            
            # Username
            if username:
                variants.add(normalize_name(username))
            
            # Email full và prefix
            if email:
                variants.add(normalize_name(email))
                email_prefix = email.split('@')[0]
                variants.add(normalize_name(email_prefix))
            
            # Tên từ username (nếu có dấu . hoặc _)
            if username:
                for separator in ['.', '_', '-']:
                    if separator in username:
                        parts = username.split(separator)
                        # Firstname
                        variants.add(normalize_name(parts[0]))
                        # Lastname
                        if len(parts) > 1:
                            variants.add(normalize_name(parts[-1]))
                        # Fullname
                        variants.add(normalize_name(' '.join(parts)))
            
            # Map tất cả variants về user_id
            for variant in variants:
                if variant:
                    user_map[variant] = user_id
        
        clickup_users_cache = user_map
        logger.success(
            "notion_sync",
            "users_cached",
            f"Created {len(user_map)} name variants for matching"
        )
        return user_map
        
    except Exception as e:
        logger.error(
            "notion_sync",
            "users_fetch_error",
            f"Error fetching ClickUp users: {e}",
            extra={"error": str(e)}
        )
        return {}

def map_notion_assignees_to_clickup(notion_assignees):
    """Map assignees từ Notion sang ClickUp IDs với matching thông minh"""
    if not notion_assignees:
        return []
    
    clickup_users = get_clickup_users()
    if not clickup_users:
        logger.warning("notion_sync", "no_users_map", "No ClickUp users available for mapping")
        return []
    
    clickup_ids = []
    matched = []
    unmatched = []
    
    for assignee in notion_assignees:
        name = assignee.get("name", "")
        email = assignee.get("email", "")
        
        user_id = None
        matched_by = None
        
        # Try match by email first (chính xác nhất)
        if email:
            normalized_email = normalize_name(email)
            if normalized_email in clickup_users:
                user_id = clickup_users[normalized_email]
                matched_by = f"email: {email}"
            else:
                # Try email prefix
                email_prefix = normalize_name(email.split('@')[0])
                if email_prefix in clickup_users:
                    user_id = clickup_users[email_prefix]
                    matched_by = f"email prefix: {email_prefix}"
        
        # Try match by name
        if not user_id and name:
            normalized_name = normalize_name(name)
            if normalized_name in clickup_users:
                user_id = clickup_users[normalized_name]
                matched_by = f"name: {name}"
            else:
                # Try first/last name
                name_parts = normalized_name.split()
                for part in name_parts:
                    if part in clickup_users:
                        user_id = clickup_users[part]
                        matched_by = f"name part: {part}"
                        break
        
        if user_id and user_id not in clickup_ids:
            clickup_ids.append(user_id)
            matched.append(f"{name or email} → {matched_by}")
        else:
            unmatched.append(name or email)
    
    if matched:
        logger.success(
            "notion_sync",
            "assignees_matched",
            f"Matched {len(matched)} assignees",
            extra={"matched": matched}
        )
    if unmatched:
        logger.warning(
            "notion_sync",
            "assignees_unmatched",
            f"Could not match {len(unmatched)} assignees",
            extra={"unmatched": unmatched}
        )
    
    return clickup_ids

# ============ NOTION API ============
def get_notion_tasks():
    """Lấy tasks từ Notion, sorted by created time"""
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
        tasks = response.json().get("results", [])
        logger.info("notion_sync", "tasks_fetched", f"Fetched {len(tasks)} tasks from Notion")
        return tasks
    except Exception as e:
        logger.error(
            "notion_sync",
            "notion_fetch_error",
            f"Error fetching Notion tasks: {e}",
            extra={"error": str(e)}
        )
        return []

def get_property_value(props, *possible_names):
    """Helper để lấy property value với nhiều tên có thể"""
    for name in possible_names:
        if name in props:
            return props[name]
    return None

def format_notion_task(page):
    """Parse và format task từ Notion page với tất cả các fields"""
    props = page.get("properties", {})
    
    # Title/Name - Required
    title_prop = get_property_value(props, "Tên công việc", "Name", "Task", "Title")
    if title_prop and title_prop.get("title"):
        name = title_prop["title"][0]["text"]["content"]
    else:
        name = "Untitled Task"
    
    # Status
    status_prop = get_property_value(props, "Trạng thái", "Status", "State")
    status = "Chưa bắt đầu"
    if status_prop:
        if status_prop.get("status"):
            status = status_prop["status"].get("name", "Chưa bắt đầu")
        elif status_prop.get("select"):
            status = status_prop["select"].get("name", "Chưa bắt đầu")
    
    # Priority
    priority_prop = get_property_value(props, "Mức độ ưu tiên", "Priority", "Ưu tiên")
    priority = "Trung bình (Medium)"
    if priority_prop and priority_prop.get("select"):
        priority = priority_prop["select"].get("name", "Trung bình (Medium)")
    
    # Deadline/Due Date
    deadline_prop = get_property_value(props, "Deadline", "Due Date", "Hạn", "Due")
    deadline = None
    if deadline_prop and deadline_prop.get("date"):
        deadline = deadline_prop["date"].get("start")
    
    # Assignees
    assignees_prop = get_property_value(props, "Phân công", "Assign", "Assignee", "Người thực hiện")
    assignees = []
    if assignees_prop and assignees_prop.get("people"):
        assignees = [
            {
                "name": p.get("name", ""),
                "email": p.get("email", "")
            }
            for p in assignees_prop["people"]
        ]
    
    # Description/Notes
    desc_prop = get_property_value(props, "Ghi chú", "Description", "Mô tả", "Notes")
    description = ""
    if desc_prop and desc_prop.get("rich_text"):
        description = desc_prop["rich_text"][0]["text"]["content"]
    
    # Metadata
    notion_id = page.get("id", "")
    created_time = page.get("created_time", "")
    
    return {
        "notion_id": notion_id,
        "name": name,
        "status": map_notion_status_to_clickup(status),
        "priority": map_notion_priority_to_clickup(priority),
        "deadline": deadline,
        "description": description,
        "assignees": assignees,
        "created_time": created_time
    }

# ============ CLICKUP API ============
def create_clickup_task(task_data):
    """Tạo task mới trong ClickUp với đầy đủ fields"""
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    
    headers = {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    # Parse deadline
    due_date = None
    if task_data["deadline"]:
        try:
            dt = datetime.fromisoformat(task_data["deadline"].replace('Z', '+00:00'))
            due_date = int(dt.timestamp() * 1000)
        except Exception as e:
            logger.warning(
                "notion_sync",
                "deadline_parse_error",
                f"Failed to parse deadline: {e}",
                extra={"deadline": task_data["deadline"]}
            )
    
    # Map assignees
    assignee_ids = map_notion_assignees_to_clickup(task_data["assignees"])
    
    # Build payload
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
        return response.json()
    except Exception as e:
        logger.error(
            "notion_sync",
            "clickup_create_error",
            f"Failed to create ClickUp task: {e}",
            extra={"task_name": task_data["name"], "error": str(e)}
        )
        return None

def update_clickup_task(task_id, task_data):
    """Update task trong ClickUp"""
    url = f"https://api.clickup.com/api/v2/task/{task_id}"
    
    headers = {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    # Parse deadline
    due_date = None
    if task_data["deadline"]:
        try:
            dt = datetime.fromisoformat(task_data["deadline"].replace('Z', '+00:00'))
            due_date = int(dt.timestamp() * 1000)
        except:
            pass
    
    # Map assignees
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
        logger.error(
            "notion_sync",
            "clickup_update_error",
            f"Failed to update ClickUp task: {e}",
            extra={"task_id": task_id, "error": str(e)}
        )
        return None

def get_clickup_task_by_notion_id(notion_id):
    """Tìm task trong ClickUp theo Notion ID"""
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    
    headers = {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        tasks = response.json().get("tasks", [])
        
        for task in tasks:
            description = task.get("description", "")
            if f"[Notion ID: {notion_id}]" in description:
                return task.get("id")
        
        return None
    except:
        return None

# ============ SYNC LOGIC ============
def sync_notion_to_clickup():
    global sync_status
    
    logger.info("notion_sync", "sync_check", "Checking for new tasks")
    
    known_data = load_known_tasks()
    known_task_ids = set(known_data.get("task_ids", []))
    is_initialized = known_data.get("initialized", False)
    
    notion_tasks = get_notion_tasks()
    if not notion_tasks:
        logger.warning("notion_sync", "no_tasks", "No tasks fetched from Notion")
        return
    
    current_task_ids = [task.get("id") for task in notion_tasks]
    
    if not is_initialized:
        logger.info(
            "notion_sync",
            "first_run",
            f"First run - Saved snapshot of {len(current_task_ids)} existing tasks",
            extra={
                "total_tasks": len(current_task_ids),
                "action": "skip_sync"
            }
        )
        
        known_data = {
            "task_ids": current_task_ids,
            "initialized": True,
            "initialized_at": datetime.now().isoformat()
        }
        save_known_tasks(known_data)
        return
    
    new_task_ids = [tid for tid in current_task_ids if tid not in known_task_ids]
    
    if not new_task_ids:
        logger.info("notion_sync", "no_new_tasks", "No new tasks detected")
        return
    
    logger.info(
        "notion_sync",
        "new_tasks_detected",
        f"Found {len(new_task_ids)} new tasks to sync",
        extra={"count": len(new_task_ids)}
    )
    
    created = 0
    updated = 0
    errors = 0
    
    for notion_page in notion_tasks:
        notion_id = notion_page.get("id")
        
        if notion_id not in new_task_ids:
            continue
        
        try:
            task_data = format_notion_task(notion_page)
            
            clickup_task_id = get_clickup_task_by_notion_id(notion_id)
            
            if clickup_task_id:
                result = update_clickup_task(clickup_task_id, task_data)
                if result:
                    updated += 1
                    logger.success(
                        "notion_sync",
                        "task_updated",
                        f"Updated task: {task_data['name']}",
                        extra={"clickup_id": clickup_task_id, "notion_id": notion_id}
                    )
                else:
                    errors += 1
            else:
                result = create_clickup_task(task_data)
                if result:
                    created += 1
                    logger.success(
                        "notion_sync",
                        "task_created",
                        f"Created task: {task_data['name']}",
                        extra={"clickup_id": result.get('id'), "notion_id": notion_id}
                    )
                else:
                    errors += 1
            
            known_task_ids.add(notion_id)
            time.sleep(0.3)
            
        except Exception as e:
            errors += 1
            logger.error(
                "notion_sync",
                "sync_error",
                f"Error syncing task: {str(e)}",
                extra={"notion_id": notion_id, "error": str(e)}
            )
            sync_status["last_error"] = str(e)
    
    known_data["task_ids"] = list(known_task_ids)
    save_known_tasks(known_data)
    
    if created > 0 or updated > 0:
        logger.success(
            "notion_sync",
            "sync_completed",
            f"Sync completed: {created} created, {updated} updated, {errors} errors",
            extra={"created": created, "updated": updated, "errors": errors}
        )
        sync_status["total_synced"] += created + updated
        if errors > 0:
            sync_status["errors"] += errors
    
    sync_status["last_sync"] = datetime.now().isoformat()

# ============ BACKGROUND SYNC THREAD ============
def background_sync_loop():
    global sync_status
    
    sync_status["running"] = True
    sync_interval = 15
    
    logger.info("notion_sync", "service_started", "Background sync thread started")
    
    # Load users cache
    users = get_clickup_users()
    logger.success(
        "notion_sync",
        "users_ready",
        f"Ready to match assignees with {len(users)} name variants"
    )
    
    while sync_status["running"]:
        try:
            sync_notion_to_clickup()
        except Exception as e:
            logger.error(
                "notion_sync",
                "sync_loop_error",
                f"Error in sync loop: {e}",
                extra={"error": str(e)}
            )
            sync_status["errors"] += 1
            sync_status["last_error"] = str(e)
        
        time.sleep(sync_interval)

# ============ FLASK ROUTES ============
@app.route('/')
def home():
    known_data = load_known_tasks()
    return jsonify({
        "status": "running",
        "service": "Notion → ClickUp Sync (Optimized v1.3)",
        "service_started": sync_status["service_started"],
        "last_sync": sync_status["last_sync"],
        "total_synced": sync_status["total_synced"],
        "errors": sync_status["errors"],
        "last_error": sync_status["last_error"],
        "known_tasks": len(known_data.get("task_ids", [])),
        "initialized": known_data.get("initialized", False),
        "data_path": KNOWN_TASKS_FILE
    })

@app.route('/health')
def health():
    return jsonify({"status": "ok"}), 200

@app.route('/status')
def status():
    known_data = load_known_tasks()
    users = get_clickup_users()
    return jsonify({
        "sync_status": sync_status,
        "known_tasks": len(known_data.get("task_ids", [])),
        "initialized": known_data.get("initialized", False),
        "initialized_at": known_data.get("initialized_at", None),
        "data_path": KNOWN_TASKS_FILE,
        "file_exists": os.path.exists(KNOWN_TASKS_FILE),
        "clickup_users_cached": len(users)
    })

@app.route('/trigger')
def trigger():
    try:
        sync_notion_to_clickup()
        return jsonify({"status": "success", "message": "Sync triggered manually"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/reset')
def reset():
    """Reset state - Xóa file và bắt đầu lại từ đầu"""
    try:
        if os.path.exists(KNOWN_TASKS_FILE):
            os.remove(KNOWN_TASKS_FILE)
            logger.info("notion_sync", "state_reset", "State file deleted, will re-initialize on next sync")
            return jsonify({
                "status": "success",
                "message": "State reset - sẽ re-initialize ở lần sync tiếp theo"
            })
        else:
            return jsonify({
                "status": "info",
                "message": "File không tồn tại"
            })
    except Exception as e:
        logger.error("notion_sync", "reset_error", f"Error resetting state: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/users')
def users():
    """View cached ClickUp users"""
    users = get_clickup_users()
    return jsonify({
        "total_variants": len(users),
        "sample_variants": list(users.keys())[:20]
    })

@app.route('/logs')
def logs():
    """View recent logs with filters"""
    limit = request.args.get('limit', 50, type=int)
    level = request.args.get('level')  # INFO, ERROR, SUCCESS, WARNING
    service = request.args.get('service', 'notion_sync')
    
    logs = logger.get_recent_logs(limit=limit, level=level, service=service)
    stats = logger.get_stats()
    
    return jsonify({
        "logs": logs,
        "stats": stats,
        "filters": {"limit": limit, "level": level, "service": service}
    })

if __name__ == '__main__':
    logger.info("system", "startup", "🚀 Notion → ClickUp Flask Sync Service v1.3 (Refactored)")
    
    # Start background sync thread
    sync_thread = threading.Thread(target=background_sync_loop, daemon=True)
    sync_thread.start()
    logger.success("system", "thread_started", "Background sync thread started successfully")
    
    # Start Flask app
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)