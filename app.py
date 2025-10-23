"""
Flask wrapper để chạy sync script như Web Service trên Render
"""

from flask import Flask, jsonify
import threading
import time
from datetime import datetime
import requests
import os
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============ CONFIGURATION ============
NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
CLICKUP_API_TOKEN = os.getenv("CLICKUP_API_TOKEN")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID")
KNOWN_TASKS_FILE = "known_tasks.json"

app = Flask(__name__)

# Global state
sync_status = {
    "running": False,
    "last_sync": None,
    "total_synced": 0,
    "errors": 0,
    "last_error": None
}

# ============ STATE MANAGEMENT ============
def load_known_tasks():
    if os.path.exists(KNOWN_TASKS_FILE):
        try:
            with open(KNOWN_TASKS_FILE, 'r') as f:
                return json.load(f)
        except:
            return {"task_ids": [], "initialized": False}
    return {"task_ids": [], "initialized": False}

def save_known_tasks(known_tasks):
    with open(KNOWN_TASKS_FILE, 'w') as f:
        json.dump(known_tasks, f, indent=2)

# ============ STATUS & PRIORITY MAPPING ============
def map_notion_status_to_clickup(notion_status):
    status_mapping = {
        "Chưa bắt đầu": "to do",
        "Đang thực hiện": "in progress",
        "Hoàn thành": "complete",
        "Đóng": "closed",
        "Not started": "to do",
        "In progress": "in progress",
        "Complete": "complete",
        "Closed": "closed"
    }
    return status_mapping.get(notion_status, "to do")

def map_notion_priority_to_clickup(notion_priority):
    priority_mapping = {
        "Cao (High)": 1,
        "High": 1,
        "Urgent": 1,
        "Trung bình (Medium)": 3,
        "Medium": 3,
        "Normal": 3,
        "Thấp (Low)": 4,
        "Low": 4,
    }
    return priority_mapping.get(notion_priority, 3)

# ============ CLICKUP USER MANAGEMENT ============
clickup_users_cache = None

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
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        teams = response.json().get("teams", [])
        
        if teams:
            team_id = teams[0]["id"]
            url = f"https://api.clickup.com/api/v2/team/{team_id}/user"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            members = response.json().get("members", [])
            
            user_map = {}
            for member in members:
                user = member.get("user", {})
                username = user.get("username", "").lower()
                email = user.get("email", "").lower()
                user_id = user.get("id")
                
                if user_id:
                    if username:
                        user_map[username] = user_id
                    if email:
                        user_map[email] = user_id
                        email_prefix = email.split('@')[0]
                        user_map[email_prefix] = user_id
            
            clickup_users_cache = user_map
            return user_map
    except Exception as e:
        print(f"⚠️  Lỗi lấy users ClickUp: {e}")
    
    return {}

def map_notion_assignees_to_clickup(notion_assignees):
    if not notion_assignees:
        return []
    
    clickup_users = get_clickup_users()
    clickup_ids = []
    
    for assignee in notion_assignees:
        name = assignee.get("name", "").lower()
        email = assignee.get("email", "").lower() if assignee.get("email") else ""
        
        if name in clickup_users:
            clickup_ids.append(clickup_users[name])
        elif email in clickup_users:
            clickup_ids.append(clickup_users[email])
    
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
        "sorts": [
            {
                "timestamp": "created_time",
                "direction": "descending"
            }
        ]
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json().get("results", [])
    except Exception as e:
        print(f"❌ Lỗi lấy data từ Notion: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
        return []

def format_notion_task(page):
    props = page.get("properties", {})
    
    title_prop = (props.get("Tên công việc", {}) or 
                  props.get("Name", {}) or 
                  props.get("Task", {})).get("title", [])
    name = title_prop[0]["text"]["content"] if title_prop else "Untitled"
    
    status_prop = (props.get("Trạng thái", {}) or 
                   props.get("Status", {})).get("status", {})
    status = status_prop.get("name", "Chưa bắt đầu") if status_prop else "Chưa bắt đầu"
    
    priority_prop = (props.get("Mức độ ưu tiên", {}) or 
                     props.get("Priority", {})).get("select", {})
    priority = priority_prop.get("name", "Trung bình (Medium)") if priority_prop else "Trung bình (Medium)"
    
    deadline_prop = (props.get("Deadline", {}) or 
                     props.get("Due Date", {})).get("date", {})
    deadline = deadline_prop.get("start") if deadline_prop else None
    
    assignees_prop = (props.get("Phân công", {}) or 
                      props.get("Assign", {}) or 
                      props.get("Assignee", {})).get("people", [])
    assignees = [{"name": p.get("name", ""), "email": p.get("email", "")} 
                 for p in assignees_prop]
    
    notion_id = page.get("id", "")
    created_time = page.get("created_time", "")
    
    ghi_chu_prop = (props.get("Ghi chú", {}) or 
                    props.get("Description", {})).get("rich_text", [])
    description = ghi_chu_prop[0]["text"]["content"] if ghi_chu_prop else ""
    
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
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Lỗi tạo task ClickUp: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Response: {e.response.text}")
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
        response = requests.put(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Lỗi update task ClickUp: {e}")
        return None

def get_clickup_task_by_notion_id(notion_id):
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    
    headers = {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(url, headers=headers)
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
    
    print(f"\n🔄 Checking for new tasks... {datetime.now().strftime('%H:%M:%S')}")
    
    known_data = load_known_tasks()
    known_task_ids = set(known_data.get("task_ids", []))
    is_initialized = known_data.get("initialized", False)
    
    notion_tasks = get_notion_tasks()
    if not notion_tasks:
        return
    
    current_task_ids = [task.get("id") for task in notion_tasks]
    
    if not is_initialized:
        print("🎯 Lần đầu chạy - Đang lưu snapshot của tasks hiện tại...")
        print(f"   📝 Tìm thấy {len(current_task_ids)} tasks có sẵn")
        print("   ⏭️  Bỏ qua việc sync các tasks này")
        print("   ✅ Từ giờ sẽ chỉ sync tasks MỚI được tạo!")
        
        known_data = {
            "task_ids": current_task_ids,
            "initialized": True,
            "initialized_at": datetime.now().isoformat()
        }
        save_known_tasks(known_data)
        return
    
    new_task_ids = [tid for tid in current_task_ids if tid not in known_task_ids]
    
    if not new_task_ids:
        print("   ✨ Không có task mới")
        return
    
    print(f"   🆕 Phát hiện {len(new_task_ids)} task mới!")
    
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
                    print(f"      🔄 Updated: {task_data['name']}")
                else:
                    errors += 1
            else:
                result = create_clickup_task(task_data)
                if result:
                    created += 1
                    print(f"      ✨ Created: {task_data['name']}")
                else:
                    errors += 1
            
            known_task_ids.add(notion_id)
            time.sleep(0.3)
            
        except Exception as e:
            print(f"      ❌ Lỗi sync task: {e}")
            errors += 1
            sync_status["last_error"] = str(e)
    
    known_data["task_ids"] = list(known_task_ids)
    save_known_tasks(known_data)
    
    if created > 0 or updated > 0:
        print(f"   ✅ Sync done: {created} created, {updated} updated")
        sync_status["total_synced"] += created + updated
        if errors > 0:
            print(f"   ⚠️  {errors} errors")
            sync_status["errors"] += errors
    
    sync_status["last_sync"] = datetime.now().isoformat()

# ============ BACKGROUND SYNC THREAD ============
def background_sync_loop():
    global sync_status
    
    sync_status["running"] = True
    sync_interval = 15
    
    print("🔍 Loading ClickUp users...")
    users = get_clickup_users()
    print(f"✅ Found {len(users)} users")
    
    while sync_status["running"]:
        try:
            sync_notion_to_clickup()
        except Exception as e:
            print(f"❌ Error in sync: {e}")
            sync_status["errors"] += 1
            sync_status["last_error"] = str(e)
        
        time.sleep(sync_interval)

# ============ FLASK ROUTES ============
@app.route('/')
def home():
    return jsonify({
        "status": "running",
        "service": "Notion → ClickUp Sync",
        "last_sync": sync_status["last_sync"],
        "total_synced": sync_status["total_synced"],
        "errors": sync_status["errors"],
        "last_error": sync_status["last_error"]
    })

@app.route('/health')
def health():
    return jsonify({"status": "ok"}), 200

@app.route('/status')
def status():
    known_data = load_known_tasks()
    return jsonify({
        "sync_status": sync_status,
        "known_tasks": len(known_data.get("task_ids", [])),
        "initialized": known_data.get("initialized", False),
        "initialized_at": known_data.get("initialized_at", None)
    })

@app.route('/trigger')
def trigger():
    try:
        sync_notion_to_clickup()
        return jsonify({"status": "success", "message": "Sync triggered manually"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Notion → ClickUp Flask Sync Service")
    print("=" * 60)
    
    # Start background sync thread
    sync_thread = threading.Thread(target=background_sync_loop, daemon=True)
    sync_thread.start()
    print("✅ Background sync thread started")
    
    # Start Flask app
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)