#!/usr/bin/env python3
"""
ClickUp Cleanup Script - Delete all tasks and re-import from tasks.md

This script deletes all tasks with the 'instruments-service' tag from a ClickUp list,
then re-imports from tasks.md cleanly.

Usage:
    python scripts/clickup_cleanup.py --api-token YOUR_TOKEN --list-id LIST_ID [--dry-run] [--source tasks.md|STATUS.md]

WARNING: This will delete ALL tasks with the 'instruments-service' tag!
"""

import argparse
import sys
import re
from pathlib import Path
from typing import Dict, List
from datetime import datetime, timezone

# Add parent directory to path to import clickup_import
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.clickup_import import ClickUpClient
from unified_cloud_services import get_secret_with_fallback
from instruments_service.config import instruments_config


class TasksMdParser:
    """Parser for tasks.md format"""

    def __init__(self, tasks_md_path: Path):
        self.tasks_md_path = tasks_md_path
        self.content = tasks_md_path.read_text()

    def parse_tasks(self) -> List[Dict]:
        """Parse tasks from tasks.md markdown format"""
        tasks = []

        # Pattern to match task headers: ### N. Task Name
        task_pattern = r"^### (\d+)\.\s+(.+)$"

        # Pattern to match task metadata lines
        status_pattern = r"\*\*Status\*\*:\s*(.+)$"
        due_pattern = r"\*\*Due\*\*:\s*(.+)$"
        owner_pattern = r"\*\*Owner\*\*:\s*(.+)$"

        lines = self.content.split("\n")
        current_task = None

        for line in lines:
            # Check for task header
            task_match = re.match(task_pattern, line)
            if task_match:
                # Save previous task if exists
                if current_task:
                    tasks.append(current_task)

                # Start new task
                task_num = task_match.group(1)
                task_name = task_match.group(2).strip()
                current_task = {
                    "name": task_name,
                    "number": task_num,
                    "status": None,
                    "due_date": None,
                    "owner": None,
                    "description": [],
                }
                continue

            if current_task:
                # Check for metadata fields
                status_match = re.search(status_pattern, line)
                if status_match:
                    current_task["status"] = status_match.group(1).strip()
                    continue

                due_match = re.search(due_pattern, line)
                if due_match:
                    due_str = due_match.group(1).strip()
                    # Parse date formats like "November 10th, 2025" or "Nov 10"
                    try:
                        # Try to parse common date formats
                        if "th" in due_str or "st" in due_str or "nd" in due_str or "rd" in due_str:
                            # Format: "November 10th, 2025"
                            date_str = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", due_str)
                            current_task["due_date"] = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
                        elif re.match(r"\w+ \d+", due_str):
                            # Format: "Nov 10" - assume current year
                            date_str = f"{due_str}, {datetime.now(timezone.utc).year}"
                            current_task["due_date"] = datetime.strptime(date_str, "%b %d, %Y").strftime("%Y-%m-%d")
                        else:
                            current_task["due_date"] = due_str
                    except (ValueError, TypeError):
                        current_task["due_date"] = due_str
                    continue

                owner_match = re.search(owner_pattern, line)
                if owner_match:
                    current_task["owner"] = owner_match.group(1).strip()
                    continue

                # Collect description lines (non-empty, non-metadata lines)
                if line.strip() and not line.strip().startswith("**") and not line.strip().startswith("|"):
                    current_task["description"].append(line.strip())

        # Don't forget the last task
        if current_task:
            tasks.append(current_task)

        return tasks


def delete_all_tasks(client: ClickUpClient, list_id: str, dry_run: bool = False):
    """Delete all tasks with 'instruments-service' tag (including archived/completed)"""
    print("🔍 Loading all tasks (including subtasks and archived)...")

    # Get workspace/team ID from list to search across all lists
    try:
        list_info = client.get_list(list_id)
        workspace_id = list_info.get("workspace", {}).get("id")
        if not workspace_id:
            print("   ⚠️  Could not get workspace ID from list - falling back to single list search")
            workspace_id = None
    except Exception as e:
        print(f"   ⚠️  Could not get list info: {e} - falling back to single list search")
        workspace_id = None

    all_tasks = []
    active_tasks = []
    archived_tasks = []

    if workspace_id:
        # Use filtered team tasks endpoint to search across ALL lists/folders by tag
        print(f"   Searching across all lists in workspace {workspace_id}...")
        try:
            # Get tasks from entire workspace (will filter by tag below)
            params = {
                "subtasks": "true",
                "archived": "false",  # Get active tasks first
            }

            result = client._request("GET", f"/team/{workspace_id}/task", params=params)
            if result:
                active_tasks = result.get("tasks", [])
                all_tasks.extend(active_tasks)

            # Get archived tasks
            params["archived"] = "true"
            result = client._request("GET", f"/team/{workspace_id}/task", params=params)
            if result:
                archived_tasks = result.get("tasks", [])
                all_tasks.extend(archived_tasks)

            print(
                f"   Found {len(active_tasks)} active task(s) and {len(archived_tasks)} archived task(s) across all lists"
            )
        except Exception as e:
            print(f"   ⚠️  Could not search workspace tasks: {e}")
            print("   Falling back to single list search...")
            workspace_id = None

    if not workspace_id:
        # Fallback: search only the specified list
        active_tasks = client.get_tasks(list_id, archived=False, include_subtasks=True)
        archived_tasks = client.get_tasks(list_id, archived=True, include_subtasks=True)
        all_tasks = active_tasks + archived_tasks
        print(f"   Found {len(active_tasks)} active task(s) and {len(archived_tasks)} archived task(s) in list")

    if not all_tasks:
        print("   ✅ No tasks found")
        return 0

    # Filter tasks with instruments-service tag
    tasks_to_delete = []
    archived_task_ids = {task.get("id") for task in archived_tasks}  # Set of archived task IDs

    for task in all_tasks:
        task_id = task.get("id")
        task_name = task.get("name", "Unknown")
        tags = [tag.get("name", "") for tag in task.get("tags", [])]
        status = task.get("status", {}).get("status", "unknown")

        if "instruments-service" in tags:
            # Handle parent field - can be string (task ID) or object with id
            parent_field = task.get("parent")
            if isinstance(parent_field, dict):
                parent_id = parent_field.get("id")
            elif isinstance(parent_field, str):
                parent_id = parent_field
            else:
                parent_id = None

            tasks_to_delete.append(
                {
                    "id": task_id,
                    "name": task_name,
                    "is_subtask": parent_id is not None,
                    "status": status,
                    "is_archived": task_id in archived_task_ids,
                }
            )

    if not tasks_to_delete:
        print("   ✅ No tasks with 'instruments-service' tag found")
        return 0

    print(f"\n🗑️  Found {len(tasks_to_delete)} task(s) with 'instruments-service' tag to delete:")
    subtask_count = sum(1 for t in tasks_to_delete if t["is_subtask"])
    parent_count = len(tasks_to_delete) - subtask_count
    completed_count = sum(
        1 for t in tasks_to_delete if t["status"].lower() in ["complete", "closed", "done", "resolved"]
    )
    archived_count = sum(1 for t in tasks_to_delete if t["is_archived"])
    print(f"   - {parent_count} parent task(s)")
    print(f"   - {subtask_count} subtask(s)")
    if completed_count > 0:
        print(f"   - {completed_count} completed task(s)")
    if archived_count > 0:
        print(f"   - {archived_count} archived task(s)")

    if dry_run:
        print("\n🔍 [DRY RUN] Would delete the following tasks:")
        for task in tasks_to_delete[:20]:  # Show first 20
            task_type = "subtask" if task["is_subtask"] else "task"
            status_info = f" [{task['status']}]" if task["status"] != "unknown" else ""
            archived_info = " [ARCHIVED]" if task["is_archived"] else ""
            print(f"   - {task_type}: {task['name']}{status_info}{archived_info} (ID: {task['id']})")
        if len(tasks_to_delete) > 20:
            print(f"   ... and {len(tasks_to_delete) - 20} more")
        return len(tasks_to_delete)

    # Delete subtasks first (to avoid orphaned subtasks)
    print("\n🗑️  Deleting tasks...")
    deleted_count = 0
    failed_count = 0

    # Sort: subtasks first, then parent tasks
    tasks_to_delete_sorted = sorted(tasks_to_delete, key=lambda x: (not x["is_subtask"], x["name"]))

    for task in tasks_to_delete_sorted:
        task_type = "subtask" if task["is_subtask"] else "task"
        try:
            if client.delete_task(task["id"]):
                deleted_count += 1
                if deleted_count % 10 == 0:
                    print(f"   ✅ Deleted {deleted_count}/{len(tasks_to_delete)} tasks...")
            else:
                failed_count += 1
                print(f"   ⚠️  Failed to delete {task_type}: {task['name']}")
        except Exception as e:
            failed_count += 1
            print(f"   ⚠️  Error deleting {task_type} '{task['name']}': {e}")

    print("\n✅ Cleanup complete!")
    print(f"   - Deleted: {deleted_count} task(s)")
    if failed_count > 0:
        print(f"   - Failed: {failed_count} task(s)")

    return deleted_count


def main():
    parser = argparse.ArgumentParser(
        description="Delete all ClickUp tasks with 'instruments-service' tag and re-import"
    )
    parser.add_argument("--api-token", help="ClickUp API token (or set CLICKUP_API_TOKEN env var)")
    parser.add_argument("--list-id", help="ClickUp List ID (or set CLICKUP_LIST_ID env var)")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run mode (don't delete, just show what would be deleted)",
    )
    parser.add_argument("--no-reimport", action="store_true", help="Don't re-import after cleanup (just delete)")
    parser.add_argument(
        "--source",
        choices=["tasks.md", "STATUS.md"],
        default="tasks.md",
        help="Source file to import from (default: tasks.md)",
    )

    args = parser.parse_args()

    # Get API token from args or Secret Manager/env via instruments_config
    api_token = args.api_token
    if not api_token:
        # Try Secret Manager via unified-cloud-services
        try:
            project_id = instruments_config.gcp_project_id
            secret_name = instruments_config.clickup_secret_name
            api_token = get_secret_with_fallback(
                secret_name=secret_name,
                project_id=project_id,
                fallback_env_var="CLICKUP_API_TOKEN",
            )
            if api_token:
                api_token = api_token.strip()
                print(f"✅ Retrieved ClickUp API key from Secret Manager (secret: {secret_name})")
        except Exception as e:
            print(f"⚠️  Secret Manager lookup failed: {e}")

    if not api_token:
        print("❌ API token not found. Set --api-token or CLICKUP_API_TOKEN env var")
        print(f"   Checked: Secret Manager ({instruments_config.clickup_secret_name})")
        print("   Checked: Environment variables via settings.py")
        print("\n💡 To store API key in Secret Manager, run:")
        print(
            "   cd ../unified-cloud-services && python scripts/store_secret.py --secret-name clickup-api-key --secret-value YOUR_TOKEN"
        )
        return 1

    # Get list ID from args or instruments_config
    list_id = args.list_id or instruments_config.clickup_list_id

    # Remove "li/" prefix if present
    if list_id and list_id.startswith("li/"):
        list_id = list_id[3:]

    if not list_id:
        print("❌ List ID not found. Set --list-id or CLICKUP_LIST_ID env var")
        print("\n📋 How to find your List ID:")
        print("   1. Open your 'Instruments Service' list in ClickUp")
        print("   2. Look at the URL in your browser")
        print("   3. The List ID is the number after '/li/' in the URL")
        print("      Example: https://app.clickup.com/12345678/v/l/li/98765432")
        print("      List ID would be: 98765432")
        return 1

    if args.dry_run:
        print("🔍 [DRY RUN MODE] - No tasks will be deleted")
    else:
        print("⚠️  WARNING: This will delete ALL tasks with 'instruments-service' tag!")
        response = input("   Type 'yes' to continue: ")
        if response.lower() != "yes":
            print("   Cancelled.")
            return 0

    print(f"✅ Using API token: {api_token[:20]}...")
    print(f"✅ Using List ID: {list_id}\n")

    client = ClickUpClient(api_token)

    # Delete all tasks
    deleted_count = delete_all_tasks(client, list_id, dry_run=args.dry_run)

    if args.dry_run:
        print("\n🔍 [DRY RUN] Would delete tasks, then re-import from source file")
        return 0

    if args.no_reimport:
        print("\n✅ Cleanup complete. Run clickup_import.py to re-import tasks.")
        return 0

    # Re-import from source file
    if deleted_count > 0 or args.source == "tasks.md":
        if args.source == "tasks.md":
            print("\n⚠️  Note: tasks.md has been merged into STATUS.md")
            print("   Redirecting to STATUS.md import...")
            args.source = "STATUS.md"  # Redirect to STATUS.md

        if args.source == "STATUS.md":
            # Re-import from STATUS.md (original behavior)
            print("\n📥 Re-importing tasks from STATUS.md...")
            print("   (Run: python scripts/clickup_import.py --clean-orphaned)")

            # Import the importer and run it
            from scripts.clickup_import import ClickUpImporter

            status_md_path = Path(__file__).parent.parent / "docs" / "STATUS.md"
            if not status_md_path.exists():
                print(f"⚠️  STATUS.md not found at {status_md_path}")
                return 1

            sprint_start = "2025-11-07"  # Default sprint start
            importer = ClickUpImporter(
                api_token,
                list_id,
                dry_run=False,
                sprint_start_date=sprint_start,
                clean_orphaned=False,
            )
            importer.import_from_status_md(status_md_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
