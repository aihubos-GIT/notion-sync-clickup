"""
Flask web service để sync Notion → ClickUp
✅ Phiên bản dành riêng cho Render (chạy bằng cron job)
"""

from flask import Flask, jsonify
import time
from datetime import datetime
import requests
import os
import json
from dotenv import load_dotenv

# ============ CONFIGURATION ============
load_dotenv()

NOTION_API_TOKEN = os.getenv("NOTION_API_TOKEN")
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID")
CLICKUP_API_TOKEN = os.getenv("CLICKUP_API_TOKEN")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID")
KNOWN_TASKS_FILE = "known_tasks.json"

app = Flask(__name__)

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

# ============ MAPPING ============
def map_notion_status_to_clickup(notion_status):
    mapping = {
        "Chưa bắt đầu": "to do",
        "Đang thực hiện": "in progress",
        "Hoàn thành": "complete",
        "Đóng": "closed",
        "Not started": "to do",
        "In progress": "in progress",
        "Complete": "complete",
        "Closed": "closed"
    }
    return mapping.get(notion_status, "to do")

def map_notion_priority_to_clickup(priority):
    mapping = {
        "Cao (High)": 1, "High": 1, "Urgent": 1,
        "Trung bình (Medium)": 3, "Medium": 3, "Normal": 3,
        "Thấp (Low)": 4, "Low": 4,
    }
    return mapping.get(priority, 3)

# ============ CLICKUP USERS ============
clickup_users_cache = None

def get_clickup_users():
    global clickup_users_cache
    if clickup_users_cache:
        return clickup_users_cache

    headers = {"Authorization": CLICKUP_API_TOKEN, "Content-Type": "application/json"}
    try:
        # Lấy team
        team_resp = requests.get("https://api.clickup.com/api/v2/team", headers=headers)
        team_resp.raise_for_status()
        teams = team_resp.json().get("teams", [])
        if not teams:
            return {}
        team_id = teams[0]["id"]

        # Lấy users
        url = f"https://api.clickup.com/api/v2/team/{team_id}/user"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        members = resp.json().get("members", [])

        user_map = {}
        for m in members:
            user = m.get("user", {})
            name = user.get("username", "").lower()
            email = user.get("email", "").lower()
            user_id = user.get("id")
            if user_id:
                if name:
                    user_map[name] = user_id
                if email:
                    user_map[email] = user_id
                    prefix = email.split('@')[0]
                    user_map[prefix] = user_id

        clickup_users_cache = user_map
        print(f"✅ Found {len(user_map)} ClickUp users")
        return user_map
    except Exception as e:
        print(f"⚠️ Error fetching ClickUp users: {e}")
        return {}

def map_notion_assignees_to_clickup(notion_assignees):
    if not notion_assignees:
        return []
    clickup_users = get_clickup_users()
    ids = []
    for a in notion_assignees:
        name = a.get("name", "").lower()
        email = a.get("email", "").lower()
        if name in clickup_users:
            ids.append(clickup_users[name])
        elif email in clickup_users:
            ids.append(clickup_users[email])
    return ids

# ============ NOTION API ============
def get_notion_tasks():
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_API_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    payload = {"sorts": [{"timestamp": "created_time", "direction": "descending"}]}
    try:
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.json().get("results", [])
    except Exception as e:
        print(f"❌ Lỗi lấy data từ Notion: {e}")
        return []

def format_notion_task(page):
    props = page.get("properties", {})
    title_prop = (props.get("Tên công việc") or props.get("Name") or props.get("Task") or {}).get("title", [])
    name = title_prop[0]["text"]["content"] if title_prop else "Untitled"
    status_prop = (props.get("Trạng thái") or props.get("Status") or {}).get("status", {})
    status = status_prop.get("name", "Chưa bắt đầu")
    priority_prop = (props.get("Mức độ ưu tiên") or props.get("Priority") or {}).get("select", {})
    priority = priority_prop.get("name", "Trung bình (Medium)")
    deadline_prop = (props.get("Deadline") or props.get("Due Date") or {}).get("date", {})
    deadline = deadline_prop.get("start") if deadline_prop else None
    assignees_prop = (props.get("Phân công") or props.get("Assign") or props.get("Assignee") or {}).get("people", [])
    assignees = [{"name": p.get("name", ""), "email": p.get("email", "")} for p in assignees_prop]
    ghi_chu_prop = (props.get("Ghi chú") or props.get("Description") or {}).get("rich_text", [])
    description = ghi_chu_prop[0]["text"]["content"] if ghi_chu_prop else ""

    return {
        "notion_id": page.get("id"),
        "name": name,
        "status": map_notion_status_to_clickup(status),
        "priority": map_notion_priority_to_clickup(priority),
        "deadline": deadline,
        "description": description,
        "assignees": assignees,
    }

# ============ CLICKUP API ============
def create_clickup_task(task_data):
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    headers = {"Authorization": CLICKUP_API_TOKEN, "Content-Type": "application/json"}

    due_date = None
    if task_data["deadline"]:
        try:
            dt = datetime.fromisoformat(task_data["deadline"].replace('Z', '+00:00'))
            due_date = int(dt.timestamp() * 1000)
        except:
            pass

    payload = {
        "name": task_data["name"],
        "description": f"[Notion ID: {task_data['notion_id']}]\n\n{task_data['description']}",
        "status": task_data["status"],
        "priority": task_data["priority"],
    }
    if due_date:
        payload["due_date"] = due_date

    assignees = map_notion_assignees_to_clickup(task_data["assignees"])
    if assignees:
        payload["assignees"] = assignees

    try:
        resp = requests.post(url, headers=headers, json=payload)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"❌ Lỗi tạo task ClickUp: {e}")
        return None

# ============ SYNC LOGIC ============
def sync_notion_to_clickup():
    global sync_status

    print(f"\n🔄 Sync started at {datetime.now().strftime('%H:%M:%S')}")
    sync_status["running"] = True

    known = load_known_tasks()
    known_ids = set(known.get("task_ids", []))
    is_initialized = known.get("initialized", False)

    notion_tasks = get_notion_tasks()
    if not notion_tasks:
        print("⚠️ Không có task nào từ Notion.")
        sync_status["running"] = False
        return

    current_ids = [t.get("id") for t in notion_tasks]

    if not is_initialized:
        print(f"📋 Lần đầu chạy: Ghi nhận {len(current_ids)} task hiện có.")
        known.update({
            "task_ids": current_ids,
            "initialized": True,
            "initialized_at": datetime.now().isoformat()
        })
        save_known_tasks(known)
        sync_status["running"] = False
        return

    new_ids = [tid for tid in current_ids if tid not in known_ids]
    if not new_ids:
        print("✨ Không có task mới.")
        sync_status["last_sync"] = datetime.now().isoformat()
        sync_status["running"] = False
        return

    print(f"🆕 Phát hiện {len(new_ids)} task mới.")
    created, errors = 0, 0

    for page in notion_tasks:
        if page.get("id") not in new_ids:
            continue
        try:
            data = format_notion_task(page)
            result = create_clickup_task(data)
            if result:
                created += 1
                print(f"✅ Created: {data['name']}")
                known_ids.add(data["notion_id"])
            else:
                errors += 1
            time.sleep(0.3)
        except Exception as e:
            print(f"❌ Lỗi sync: {e}")
            errors += 1
            sync_status["last_error"] = str(e)

    known["task_ids"] = list(known_ids)
    save_known_tasks(known)
    sync_status.update({
        "last_sync": datetime.now().isoformat(),
        "total_synced": sync_status["total_synced"] + created,
        "errors": sync_status["errors"] + errors,
        "running": False
    })

    print(f"🎯 Sync xong: {created} task mới, {errors} lỗi.\n")

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
    data = load_known_tasks()
    return jsonify({
        "sync_status": sync_status,
        "known_tasks": len(data.get("task_ids", [])),
        "initialized": data.get("initialized", False),
        "initialized_at": data.get("initialized_at", None)
    })

@app.route('/trigger')
def trigger():
    try:
        sync_notion_to_clickup()
        return jsonify({"status": "success", "message": "Sync triggered manually"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ============ ENTRY POINT ============
if __name__ == '__main__':
    print("=" * 60)
    print("🚀 Notion → ClickUp Flask Sync Service (Render Mode)")
    print("=" * 60)
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)
