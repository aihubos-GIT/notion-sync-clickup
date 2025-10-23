"""
Notion to ClickUp Realtime Sync Script
Chỉ sync tasks MỚI được tạo sau khi script chạy (không sync tasks cũ)
"""

import requests
import time
from datetime import datetime
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

# File tracking các tasks đã biết (để detect tasks mới)
KNOWN_TASKS_FILE = "known_tasks.json"

# ============ STATE MANAGEMENT ============
def load_known_tasks():
    """Load danh sách các Notion task IDs đã biết"""
    if os.path.exists(KNOWN_TASKS_FILE):
        try:
            with open(KNOWN_TASKS_FILE, 'r') as f:
                return json.load(f)
        except:
            return {"task_ids": [], "initialized": False}
    return {"task_ids": [], "initialized": False}

def save_known_tasks(known_tasks):
    """Lưu danh sách Notion task IDs"""
    with open(KNOWN_TASKS_FILE, 'w') as f:
        json.dump(known_tasks, f, indent=2)

# ============ STATUS & PRIORITY MAPPING ============
def map_notion_status_to_clickup(notion_status):
    """Map Notion status sang ClickUp status"""
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
    """Map Notion priority sang ClickUp priority"""
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
    """Lấy danh sách users từ ClickUp workspace"""
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
    """Map Notion assignees sang ClickUp user IDs"""
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
    """Lấy danh sách tasks từ Notion Database"""
    url = f"https://api.notion.com/v1/databases/{NOTION_DATABASE_ID}/query"
    
    headers = {
        "Authorization": f"Bearer {NOTION_API_TOKEN}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28"
    }
    
    # Sort by created_time desc để lấy tasks mới nhất trước
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
    """Format task data từ Notion"""
    props = page.get("properties", {})
    
    # Lấy title
    title_prop = (props.get("Tên công việc", {}) or 
                  props.get("Name", {}) or 
                  props.get("Task", {})).get("title", [])
    name = title_prop[0]["text"]["content"] if title_prop else "Untitled"
    
    # Lấy status
    status_prop = (props.get("Trạng thái", {}) or 
                   props.get("Status", {})).get("status", {})
    status = status_prop.get("name", "Chưa bắt đầu") if status_prop else "Chưa bắt đầu"
    
    # Lấy priority
    priority_prop = (props.get("Mức độ ưu tiên", {}) or 
                     props.get("Priority", {})).get("select", {})
    priority = priority_prop.get("name", "Trung bình (Medium)") if priority_prop else "Trung bình (Medium)"
    
    # Lấy deadline
    deadline_prop = (props.get("Deadline", {}) or 
                     props.get("Due Date", {})).get("date", {})
    deadline = deadline_prop.get("start") if deadline_prop else None
    
    # Lấy assignees
    assignees_prop = (props.get("Phân công", {}) or 
                      props.get("Assign", {}) or 
                      props.get("Assignee", {})).get("people", [])
    assignees = [{"name": p.get("name", ""), "email": p.get("email", "")} 
                 for p in assignees_prop]
    
    # Lấy IDs và timestamps
    notion_id = page.get("id", "")
    created_time = page.get("created_time", "")
    
    # Lấy description
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
    """Tạo task mới trong ClickUp"""
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    
    headers = {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    # Convert deadline
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
    """Update task trong ClickUp"""
    url = f"https://api.clickup.com/api/v2/task/{task_id}"
    
    headers = {
        "Authorization": CLICKUP_API_TOKEN,
        "Content-Type": "application/json"
    }
    
    # Convert deadline
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
        response = requests.put(url, headers=headers, json=payload)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"❌ Lỗi update task ClickUp: {e}")
        return None

def get_clickup_task_by_notion_id(notion_id):
    """Tìm ClickUp task theo Notion ID"""
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
    """Chỉ sync tasks MỚI - không sync tasks có sẵn"""
    print(f"\n🔄 Checking for new tasks... {datetime.now().strftime('%H:%M:%S')}")
    
    # Load danh sách tasks đã biết
    known_data = load_known_tasks()
    known_task_ids = set(known_data.get("task_ids", []))
    is_initialized = known_data.get("initialized", False)
    
    # Lấy tasks từ Notion
    notion_tasks = get_notion_tasks()
    if not notion_tasks:
        return
    
    current_task_ids = [task.get("id") for task in notion_tasks]
    
    # Lần đầu chạy: Chỉ lưu danh sách tasks hiện tại, KHÔNG sync
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
    
    # Tìm tasks MỚI (chưa có trong known_task_ids)
    new_task_ids = [tid for tid in current_task_ids if tid not in known_task_ids]
    
    if not new_task_ids:
        print("   ✨ Không có task mới")
        return
    
    print(f"   🆕 Phát hiện {len(new_task_ids)} task mới!")
    
    # Sync chỉ các tasks mới
    created = 0
    updated = 0
    errors = 0
    
    for notion_page in notion_tasks:
        notion_id = notion_page.get("id")
        
        # Skip tasks cũ
        if notion_id not in new_task_ids:
            continue
        
        try:
            task_data = format_notion_task(notion_page)
            
            # Check xem đã có trong ClickUp chưa (case update)
            clickup_task_id = get_clickup_task_by_notion_id(notion_id)
            
            if clickup_task_id:
                # Update
                result = update_clickup_task(clickup_task_id, task_data)
                if result:
                    updated += 1
                    print(f"      🔄 Updated: {task_data['name']}")
                else:
                    errors += 1
            else:
                # Create new
                result = create_clickup_task(task_data)
                if result:
                    created += 1
                    print(f"      ✨ Created: {task_data['name']}")
                else:
                    errors += 1
            
            # Thêm vào known tasks
            known_task_ids.add(notion_id)
            
            time.sleep(0.3)
            
        except Exception as e:
            print(f"      ❌ Lỗi sync task: {e}")
            errors += 1
    
    # Lưu lại known tasks
    known_data["task_ids"] = list(known_task_ids)
    save_known_tasks(known_data)
    
    if created > 0 or updated > 0:
        print(f"   ✅ Sync done: {created} created, {updated} updated")
        if errors > 0:
            print(f"   ⚠️  {errors} errors")

# ============ MAIN LOOP ============
def main():
    """Chạy sync loop"""
    print("=" * 60)
    print("🚀 Notion → ClickUp Realtime Sync (NEW TASKS ONLY)")
    print("=" * 60)
    
    # Kiểm tra env vars
    required_vars = {
        "NOTION_API_TOKEN": NOTION_API_TOKEN,
        "NOTION_DATABASE_ID": NOTION_DATABASE_ID,
        "CLICKUP_API_TOKEN": CLICKUP_API_TOKEN,
        "CLICKUP_LIST_ID": CLICKUP_LIST_ID
    }
    
    for var_name, var_value in required_vars.items():
        if not var_value:
            print(f"❌ ERROR: Thiếu {var_name} trong file .env")
            return
    
    print(f"📊 Notion Database: {NOTION_DATABASE_ID}")
    print(f"📊 ClickUp List: {CLICKUP_LIST_ID}")
    print(f"⏱️  Check interval: 15 giây")
    print(f"💾 Tracking file: {KNOWN_TASKS_FILE}")
    print("\n⚡ CHẾ ĐỘ REALTIME - CHỈ SYNC TASKS MỚI:")
    print("   • Lần chạy đầu: Lưu snapshot (không sync)")
    print("   • Các lần sau: Chỉ sync tasks mới thêm vào")
    print("\n💡 Nhấn Ctrl+C để dừng")
    print("=" * 60)
    
    # Load ClickUp users
    print("\n🔍 Đang load ClickUp users...")
    users = get_clickup_users()
    print(f"✅ Tìm thấy {len(users)} users")
    
    # Chạy sync đầu tiên
    try:
        sync_notion_to_clickup()
    except Exception as e:
        print(f"❌ Lỗi: {e}")
    
    # Loop với interval 15s
    sync_interval = 15
    while True:
        try:
            time.sleep(sync_interval)
            sync_notion_to_clickup()
        except KeyboardInterrupt:
            print("\n\n👋 Đã dừng script. Bye!")
            break
        except Exception as e:
            print(f"❌ Lỗi: {e}")
            time.sleep(sync_interval)

if __name__ == "__main__":
    main()