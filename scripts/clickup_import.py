#!/usr/bin/env python3
"""
ClickUp API Integration Script for Instruments Service STATUS.md

This script parses STATUS.md and creates tasks in ClickUp via API, including:
- Main milestone tasks
- Subtasks with proper hierarchy
- Task dependencies
- Custom fields (Coverage %, Test Coverage %, DRY Compliance %, Week, Strategy)
- Tags
- Due dates and statuses

Usage:
    python scripts/clickup_import.py --api-token YOUR_TOKEN --list-id LIST_ID [--space-id SPACE_ID] [--dry-run]

Requirements:
    - ClickUp API token (get from https://app.clickup.com/settings/apps)
    - List ID (found in ClickUp URL: https://app.clickup.com/LIST_ID)
    - Optional: Space ID (for creating lists)
"""

import argparse
import json
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import requests
from unified_cloud_services import get_secret_with_fallback
from instruments_service.config import instruments_config


class ClickUpRateLimiter:
    """Handles rate limiting for ClickUp API (100 requests/minute on free plan)"""

    def __init__(self, requests_per_minute: int = 100):
        self.requests_per_minute = requests_per_minute
        self.requests = []

    def wait_if_needed(self):
        """Wait if we're approaching rate limit"""
        now = time.time()
        # Remove requests older than 1 minute
        self.requests = [req_time for req_time in self.requests if now - req_time < 60]

        if len(self.requests) >= self.requests_per_minute:
            # Wait until oldest request is 1 minute old
            sleep_time = 60 - (now - self.requests[0]) + 0.1
            if sleep_time > 0:
                print(f"⏳ Rate limit: waiting {sleep_time:.1f}s...")
                time.sleep(sleep_time)

        self.requests.append(time.time())


class ClickUpClient:
    """ClickUp API client with rate limiting"""

    BASE_URL = "https://api.clickup.com/api/v2"

    def __init__(self, api_token: str, rate_limit: int = 100):
        self.api_token = api_token
        self.headers = {"Authorization": api_token, "Content-Type": "application/json"}
        self.rate_limiter = ClickUpRateLimiter(rate_limit)

    def _request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict]:
        """Make API request with rate limiting"""
        self.rate_limiter.wait_if_needed()

        url = f"{self.BASE_URL}/{endpoint.lstrip('/')}"
        response = requests.request(method, url, headers=self.headers, **kwargs)

        if response.status_code == 429:
            # Rate limited - wait and retry
            retry_after = int(response.headers.get("Retry-After", 60))
            print(f"⏳ Rate limited. Waiting {retry_after}s...")
            time.sleep(retry_after)
            return self._request(method, endpoint, **kwargs)

        # For debugging: print response for 400 errors
        if response.status_code == 400:
            try:
                error_detail = response.json()
                print(f"   🔍 API Error Details: {json.dumps(error_detail, indent=2)[:500]}")
            except:
                print(f"   🔍 API Error Response: {response.text[:500]}")

        response.raise_for_status()

        # DELETE requests return 204 No Content with empty body
        if method == "DELETE" and response.status_code == 204:
            return None

        # Check if response has content before parsing JSON
        if not response.text.strip():
            return None

        try:
            return response.json()
        except ValueError:
            # Empty or invalid JSON response
            return None

    def delete_task(self, task_id: str) -> bool:
        """Delete a task"""
        try:
            self._request("DELETE", f"/task/{task_id}")
            # DELETE returns 204 No Content (empty body) on success
            return True
        except Exception as e:
            print(f"⚠️  Could not delete task {task_id}: {e}")
            return False

    def get_tasks(
        self, list_id: str, archived: bool = False, include_subtasks: bool = False
    ) -> List[Dict]:
        """
        Get all tasks from a list

        Args:
            list_id: ClickUp list ID
            archived: Whether to include archived tasks
            include_subtasks: Whether to include subtasks in the response

        Returns:
            List of task dictionaries
        """
        try:
            params = {"archived": str(archived).lower()}
            if include_subtasks:
                params["subtasks"] = "true"
            result = self._request("GET", f"/list/{list_id}/task", params=params)
            if result is None:
                return []
            return result.get("tasks", [])
        except Exception as e:
            print(f"⚠️  Could not get tasks from list: {e}")
            return []

    def get_list(self, list_id: str) -> Dict:
        """Get list details"""
        result = self._request("GET", f"/list/{list_id}")
        if result is None:
            raise ValueError("Empty response from ClickUp API")
        return result

    def create_task(self, list_id: str, task_data: Dict) -> Dict:
        """Create a task"""
        result = self._request("POST", f"/list/{list_id}/task", json=task_data)
        if result is None:
            raise ValueError("Empty response from ClickUp API")
        return result

    def update_task(self, task_id: str, task_data: Dict) -> Dict:
        """Update a task"""
        result = self._request("PUT", f"/task/{task_id}", json=task_data)
        if result is None:
            raise ValueError("Empty response from ClickUp API")
        return result

    def create_custom_field(self, list_id: str, field_data: Dict) -> Dict:
        """Create a custom field"""
        # Ensure 'type' is at the top level (required by API)
        payload = {"name": field_data["name"], "type": field_data["type"]}
        if "type_config" in field_data:
            payload["type_config"] = field_data["type_config"]

        result = self._request("POST", f"/list/{list_id}/field", json=payload)
        if result is None:
            raise ValueError("Empty response from ClickUp API")
        return result

    def get_custom_fields(self, list_id: str) -> List[Dict]:
        """Get custom fields for a list"""
        result = self._request("GET", f"/list/{list_id}/field")
        if result is None:
            return []
        return result.get("fields", [])


class StatusMdParser:
    """Parses STATUS.md to extract tasks, subtasks, and dependencies"""

    def __init__(self, status_md_path: Path, service_tag: str = ""):
        self.status_md_path = status_md_path
        self.content = status_md_path.read_text()
        self.service_tag = service_tag or "instruments-service"  # Default fallback

    def parse_milestones(self) -> List[Dict]:
        """Parse milestone tasks from Timeline Tracking section"""
        milestones = []

        # Find the Timeline Tracking table
        pattern = (
            r"\|\s*Milestone\s*\|\s*Target Date\s*\|\s*Actual Date\s*\|\s*Status\s*\|\s*Notes\s*\|"
        )
        match = re.search(pattern, self.content)
        if not match:
            print("⚠️  WARNING: Timeline Tracking table not found in STATUS.md")
            print("   Expected format: | Milestone | Target Date | Actual Date | Status | Notes |")
            return milestones

        # Extract table rows
        lines = self.content[match.end() :].split("\n")
        rows_parsed = 0
        rows_skipped = 0

        for line in lines[:20]:  # Limit to reasonable number
            if not line.strip() or not line.startswith("|"):
                continue

            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 6:
                rows_skipped += 1
                continue

            milestone_name = parts[1]
            target_date = parts[2]
            actual_date = parts[3]
            status = parts[4]
            notes = parts[5]

            # Skip header row
            if milestone_name == "Milestone" or "---" in milestone_name:
                continue

            # Strip markdown formatting from milestone name (**text** -> text)
            milestone_name = re.sub(r"\*\*(.+?)\*\*", r"\1", milestone_name).strip()

            # Parse status
            status_clean = status.replace("✅", "").replace("⏳", "").strip().lower()
            if "complete" in status_clean:
                status_value = "complete"
            elif "planned" in status_clean:
                status_value = "to do"
            else:
                status_value = "to do"

            # Parse dates
            due_date = None
            if target_date and target_date != "TBD" and target_date != "N/A":
                try:
                    # Handle "Week 5-6" format - will be converted later by importer
                    if "Week" in target_date:
                        due_date = target_date  # Keep as string, will convert later
                    else:
                        due_date = int(
                            datetime.strptime(target_date, "%Y-%m-%d").timestamp() * 1000
                        )
                except:
                    pass

            milestones.append(
                {
                    "name": milestone_name,
                    "status": status_value,
                    "due_date": due_date,
                    "notes": notes,
                    "actual_date": actual_date if actual_date != "N/A" else None,
                    "week": (
                        target_date if "Week" in str(target_date) else None
                    ),  # Store week string
                }
            )
            rows_parsed += 1

        if rows_parsed == 0:
            print("⚠️  WARNING: No milestone rows parsed from Timeline Tracking table")
            print("   Check table format - each row should have 5 columns separated by |")
        elif rows_skipped > 0:
            print(f"⚠️  WARNING: {rows_skipped} table row(s) skipped due to formatting issues")

        return milestones

    def parse_completed_milestones(self) -> List[Dict]:
        """Parse completed milestone tasks from Completed Milestones table"""
        completed_milestones = []

        # Find the Completed Milestones table
        pattern = r"\*\*Completed Milestones\*\*"
        match = re.search(pattern, self.content)
        if not match:
            # Try alternative pattern
            pattern = r"## Completed Milestones|### Completed Milestones"
            match = re.search(pattern, self.content)

        if not match:
            print("⚠️  WARNING: Completed Milestones table not found in STATUS.md")
            return completed_milestones

        # Find the table header after the section
        content_after = self.content[match.end() :]
        table_pattern = (
            r"\|\s*Milestone\s*\|\s*Target Date\s*\|\s*Actual Date\s*\|\s*Status\s*\|\s*Notes\s*\|"
        )
        table_match = re.search(table_pattern, content_after)

        if not table_match:
            print("⚠️  WARNING: Completed Milestones table format not found")
            return completed_milestones

        # Extract table rows
        lines = content_after[table_match.end() :].split("\n")
        rows_parsed = 0
        rows_skipped = 0

        for line in lines[:20]:  # Limit to reasonable number
            if not line.strip() or not line.startswith("|"):
                continue

            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 6:
                rows_skipped += 1
                continue

            milestone_name = parts[1]
            target_date = parts[2]
            actual_date = parts[3]
            status = parts[4]
            notes = parts[5]

            # Skip header row
            if milestone_name == "Milestone" or "---" in milestone_name:
                continue

            # Only parse completed milestones
            status_clean = status.replace("✅", "").replace("⏳", "").strip().lower()
            if "complete" not in status_clean:
                continue

            # Parse dates
            due_date = None
            if actual_date and actual_date != "TBD" and actual_date != "N/A":
                try:
                    # Handle "Week 5-6" format
                    if "Week" in actual_date:
                        due_date = actual_date  # Keep as string, will convert later
                    else:
                        due_date = int(
                            datetime.strptime(actual_date, "%Y-%m-%d").timestamp() * 1000
                        )
                except:
                    pass

            completed_milestones.append(
                {
                    "name": milestone_name,
                    "status": "complete",
                    "due_date": due_date,
                    "notes": notes,
                    "actual_date": actual_date if actual_date != "N/A" else None,
                    "week": (
                        target_date if "Week" in str(target_date) else None
                    ),  # Store week string
                }
            )
            rows_parsed += 1

        if rows_parsed == 0:
            print("⚠️  WARNING: No completed milestone rows parsed")
        elif rows_skipped > 0:
            print(
                f"⚠️  WARNING: {rows_skipped} completed milestone row(s) skipped due to formatting issues"
            )

        return completed_milestones

    def parse_priority_tasks(self) -> List[Dict]:
        """Parse tasks from priority sections (High/Medium/Low Priority Tasks)"""
        tasks = []

        # Find Next Steps section
        pattern = r"## Next Steps"
        match = re.search(pattern, self.content)
        if not match:
            print("⚠️  WARNING: '## Next Steps' section not found in STATUS.md")
            return tasks

        content_after = self.content[match.end() :]

        # Find all priority sections
        priority_sections = [
            (r"### 🔴 High Priority Tasks", "High"),
            (r"### 🟡 Medium Priority Tasks", "Medium"),
            (r"### 🟢 Low Priority Tasks", "Low"),
        ]

        for section_pattern, priority_level in priority_sections:
            section_match = re.search(section_pattern, content_after)
            if not section_match:
                continue

            section_content = content_after[section_match.end() :]

            # Stop at next section (### or ##)
            next_section_match = re.search(r"\n(###|##)", section_content)
            if next_section_match:
                section_content = section_content[: next_section_match.start()]

            lines = section_content.split("\n")
            current_parent = None
            parent_task_data = None

            for line_num, line in enumerate(lines, start=1):
                # Skip empty lines
                if not line.strip():
                    continue

                # Stop if we hit a new section or non-task content
                if line.strip().startswith("#"):
                    break

                # Pattern: - [ ] Task Name - `Date` - `Owner: Name` - `Priority: Level` - `Blocks: ...` - `Dependencies: ...` - `Note: ...` (optional)
                # Also handle numbered prefixes: - [ ] 1.) Task Name - ...
                # Task name can contain dashes, so we need to match up to the first backtick-enclosed field
                # Use a more flexible pattern that matches task name up to " - `" (space-dash-space-backtick)
                task_pattern = r"- \[ \] (\d+\.\)\s*)?(.+?) - `([^`]+)` - `Owner:\s*([^`]+)` - `Priority:\s*([^`]+)` - `Blocks:\s*([^`]+)` - `Dependencies:\s*([^`]+)`(?:\s*-\s*`Note:\s*([^`]+)`)?"
                task_match = re.match(task_pattern, line)

                if task_match:
                    task_match.group(1)  # e.g., "1.) "
                    task_name = task_match.group(2).strip()
                    due_date_str = task_match.group(3).strip()
                    owner = task_match.group(4).strip()
                    task_match.group(5).strip()
                    blocks = task_match.group(6).strip()
                    dependencies = task_match.group(7).strip()

                    # Parse due date
                    due_date = None
                    if due_date_str and due_date_str != "TBD" and due_date_str != "N/A":
                        try:
                            due_date = int(
                                datetime.strptime(due_date_str, "%Y-%m-%d").timestamp() * 1000
                            )
                        except:
                            pass

                    # Map priority to ClickUp priority (1=Urgent, 2=High, 3=Normal, 4=Low)
                    priority_map = {
                        "High": 2,
                        "Medium": 3,
                        "Low": 4,
                    }
                    clickup_priority = priority_map.get(priority_level, 3)

                    task_data = {
                        "name": task_name,
                        "due_date": due_date,
                        "owner": owner,
                        "priority": clickup_priority,
                        "priority_level": priority_level,
                        "blocks": blocks if blocks != "None" else None,
                        "dependencies": dependencies if dependencies != "None" else None,
                        "subtasks": [],
                    }

                    # This is a parent task
                    current_parent = task_name
                    parent_task_data = task_data
                    tasks.append(task_data)

                # Check for subtask (indented with 2 spaces)
                elif current_parent and line.strip().startswith("  - [ ]"):
                    subtask_name = line.strip()[6:].strip()  # Remove "- [ ] " prefix
                    if parent_task_data:
                        parent_task_data["subtasks"].append(subtask_name)
                elif line.strip() and not line.startswith("  "):
                    # End of current parent's subtasks
                    current_parent = None
                    parent_task_data = None

        if len(tasks) == 0:
            print("⚠️  WARNING: No priority tasks found in Next Steps section")
            print(
                "   Expected format: - [ ] Task Name - `Date` - `Owner: Name` - `Priority: Level` - `Blocks: ...` - `Dependencies: ...`"
            )
        else:
            print(f"   Found {len(tasks)} priority task(s)")

        return tasks

    def parse_dependencies(self) -> List[Dict]:
        """Parse dependency tasks from Dependencies section"""
        dependencies = []

        # Find Dependencies section
        pattern = r"\*\*Dependencies\*\*:"
        match = re.search(pattern, self.content)
        if not match:
            # Try alternative pattern
            pattern = r"## Dependencies|### Dependencies"
            match = re.search(pattern, self.content)

        if not match:
            print("⚠️  WARNING: Dependencies section not found in STATUS.md")
            return dependencies

        content_after = self.content[match.end() :]

        # Parse dependency lines: - `name`: `Status` - `Blocking` - ✅/⏳ description
        # Pattern: - `name`: `Status` - `Blocking` - ✅/⏳ description
        dep_pattern = r"- `([^`]+)`: `([^`]+)` - `([^`]+)` - (.*)"

        lines = content_after.split("\n")
        for line in lines[:20]:  # Limit to reasonable number
            if not line.strip():
                continue

            dep_match = re.match(dep_pattern, line)
            if dep_match:
                dep_name = dep_match.group(1).strip()
                dep_status = dep_match.group(2).strip()
                dep_blocking = dep_match.group(3).strip()
                dep_description = dep_match.group(4).strip()

                # Parse status
                status_clean = dep_status.lower()
                if "complete" in status_clean:
                    status_value = "complete"
                    tags = [self.service_tag, "dependency", "complete"]
                else:
                    status_value = "to do"
                    tags = [self.service_tag, "dependency", "planned"]

                dependencies.append(
                    {
                        "name": dep_name,
                        "status": status_value,
                        "tags": tags,
                        "notes": f"{dep_blocking} - {dep_description}",
                        "blocking": dep_blocking,
                    }
                )

        if len(dependencies) == 0:
            print("⚠️  WARNING: No dependencies parsed from Dependencies section")
        else:
            print(f"   Found {len(dependencies)} dependency/dependencies")

        return dependencies

    def parse_subtasks(self) -> Dict[str, List[Dict]]:
        """Parse subtasks from Next Steps section, grouped by parent"""
        subtasks = {}

        # Find Next Steps section
        pattern = r"## Next Steps"
        match = re.search(pattern, self.content)
        if not match:
            print("⚠️  WARNING: '## Next Steps' section not found in STATUS.md")
            return subtasks

        content_after = self.content[match.end() :]

        # Find Planned section
        planned_match = re.search(r"### 📋 Planned", content_after)
        if not planned_match:
            print("⚠️  WARNING: '### 📋 Planned' subsection not found in STATUS.md")
            return subtasks

        planned_content = content_after[planned_match.end() :]

        # Extract parent tasks and their subtasks
        # Pattern: - [ ] Parent Task - `Week X-Y` - `Dependencies: ...`
        parent_pattern = r"- \[ \] ([^-]+) - `([^`]+)` - `Dependencies: ([^`]+)`"
        subtask_pattern = r"  - \[ \] (.+)"

        current_parent = None
        parents_found = 0
        subtasks_found = 0
        malformed_lines = []

        for line_num, line in enumerate(planned_content.split("\n"), start=1):
            # Check for parent task
            parent_match = re.match(parent_pattern, line)
            if parent_match:
                parent_name = parent_match.group(1).strip()
                week = parent_match.group(2).strip()
                dependencies = parent_match.group(3).strip()
                current_parent = parent_name
                subtasks[current_parent] = {
                    "week": week,
                    "dependencies": dependencies,
                    "subtasks": [],
                }
                parents_found += 1
                continue

            # Check for subtask
            if current_parent:
                subtask_match = re.match(subtask_pattern, line)
                if subtask_match:
                    subtask_name = subtask_match.group(1).strip()
                    subtasks[current_parent]["subtasks"].append(
                        {"name": subtask_name, "parent": current_parent}
                    )
                    subtasks_found += 1
                elif (
                    line.strip()
                    and not line.startswith("-")
                    and not line.startswith("#")
                    and not line.startswith("|")
                ):
                    # End of this parent's subtasks
                    current_parent = None
            elif line.strip() and line.startswith("- [ ]") and not re.match(parent_pattern, line):
                # Malformed parent task line
                malformed_lines.append((line_num, line.strip()[:80]))

        if parents_found == 0:
            print("⚠️  WARNING: No parent tasks found in Planned section")
            print("   Expected format: - [ ] Task Name - `Week X-Y` - `Dependencies: ...`")
        elif subtasks_found == 0 and parents_found > 0:
            print(f"⚠️  WARNING: Found {parents_found} parent task(s) but no subtasks")
            print("   Subtasks should be indented with 2 spaces:   - [ ] Subtask Name")

        if malformed_lines:
            print(f"⚠️  WARNING: {len(malformed_lines)} malformed parent task line(s) found:")
            for line_num, line_preview in malformed_lines[:5]:  # Show first 5
                print(f"   Line ~{line_num}: {line_preview}...")
            print("   Expected format: - [ ] Task Name - `Week X-Y` - `Dependencies: ...`")

        return subtasks

    def parse_in_progress_tasks(self) -> List[Dict]:
        """Parse in-progress tasks"""
        tasks = []

        pattern = r"### 🔄 In Progress"
        match = re.search(pattern, self.content)
        if not match:
            print("⚠️  WARNING: '### 🔄 In Progress' section not found in STATUS.md")
            return tasks

        content_after = self.content[match.end() :]

        # Stop at next section (### or ##)
        next_section_match = re.search(r"\n(###|##)", content_after)
        if next_section_match:
            content_after = content_after[: next_section_match.start()]

        lines = content_after.split("\n")

        malformed_lines = []
        for line_num, line in enumerate(lines, start=1):
            # Skip empty lines
            if not line.strip():
                continue

            # Stop if we hit a new section or non-task content
            if line.strip().startswith("#"):
                break

            # Pattern: - [ ] Task Name - `Date` - `Owner: ...`
            match_obj = re.match(r"- \[ \] ([^-]+) - `([^`]+)` - `Owner: ([^`]+)`", line)
            if match_obj:
                tasks.append(
                    {
                        "name": match_obj.group(1).strip(),
                        "due_date": match_obj.group(2).strip(),
                        "owner": match_obj.group(3).strip(),
                        "status": "in progress",
                    }
                )
            elif line.strip().startswith("- [ ]"):
                # Malformed in-progress task (has checkbox but wrong format)
                malformed_lines.append((line_num, line.strip()[:80]))

        if malformed_lines:
            print(f"⚠️  WARNING: {len(malformed_lines)} malformed in-progress task line(s):")
            for line_num, line_preview in malformed_lines:
                print(f"   Line ~{line_num}: {line_preview}...")
            print("   Expected format: - [ ] Task Name - `Date` - `Owner: Name`")

        return tasks

    def parse_data_catalogue(self) -> List[Dict]:
        """Parse Data Catalogue table from STATUS.md"""
        catalogue_items = []

        # Find Data Catalogue section
        pattern = r"### Data Catalogue"
        match = re.search(pattern, self.content)
        if not match:
            print("⚠️  WARNING: '### Data Catalogue' section not found in STATUS.md")
            return catalogue_items

        # Find table header
        content_after = self.content[match.end() :]
        table_pattern = r"\|\s*Data Type\s*\|\s*Strategy\s*\|\s*Date From\s*\|\s*Date To\s*\|\s*Status\s*\|\s*Notes\s*\|"
        table_match = re.search(table_pattern, content_after)

        if not table_match:
            print("⚠️  WARNING: Data Catalogue table format not found")
            return catalogue_items

        # Extract table rows
        lines = content_after[table_match.end() :].split("\n")
        rows_parsed = 0
        rows_skipped = 0

        for line in lines[:30]:  # Limit to reasonable number
            if not line.strip() or not line.startswith("|"):
                continue

            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 7:
                rows_skipped += 1
                continue

            data_type = parts[1]
            strategy = parts[2]
            date_from = parts[3]
            date_to = parts[4]
            status = parts[5]
            notes = parts[6]

            # Skip header row
            if data_type == "Data Type" or "---" in data_type:
                continue

            # Extract coverage % from notes if present (e.g., "~60%", "100%")
            coverage_pct = None
            if notes:
                coverage_match = re.search(r"(\d+(?:\.\d+)?)%", notes)
                if coverage_match:
                    try:
                        coverage_pct = float(coverage_match.group(1))
                    except:
                        pass

            # Handle multi-strategy entries (e.g., "Delta-One ML / TradFi")
            strategies = []
            if "/" in strategy:
                # Split by "/" and clean up
                strategies = [s.strip() for s in strategy.split("/")]
            else:
                strategies = [strategy.strip()]

            # Normalize strategy names to match milestone names
            normalized_strategies = []
            for s in strategies:
                # Map to milestone name format
                if s == "Delta-One ML":
                    normalized_strategies.append("ML Delta-One")
                elif s == "Crypto Options":
                    normalized_strategies.append("Crypto Options")
                elif s == "TradFi Options":
                    normalized_strategies.append("TradFi Options")
                elif s == "DeFi":
                    normalized_strategies.append("DeFi")
                elif s == "TradFi":
                    normalized_strategies.append("TradFi")
                elif s == "Sports Betting":
                    normalized_strategies.append("Sports Betting")
                else:
                    # Default: use as-is
                    normalized_strategies.append(s)

            strategies = normalized_strategies

            catalogue_items.append(
                {
                    "data_type": data_type,
                    "strategies": strategies,
                    "date_from": date_from if date_from != "N/A" else None,
                    "date_to": date_to if date_to != "N/A" else None,
                    "status": status,
                    "notes": notes,
                    "coverage_pct": coverage_pct,
                }
            )
            rows_parsed += 1

        if rows_parsed == 0:
            print("⚠️  WARNING: No data catalogue rows parsed")
        elif rows_skipped > 0:
            print(
                f"⚠️  WARNING: {rows_skipped} data catalogue row(s) skipped due to formatting issues"
            )

        return catalogue_items

    def parse_process_status(self) -> List[Dict]:
        """Parse Process Status Tasks table from STATUS.md"""
        process_items = []

        # Find Process Status Tasks section
        pattern = r"### Process Status Tasks"
        match = re.search(pattern, self.content)
        if not match:
            print("⚠️  WARNING: '### Process Status Tasks' section not found in STATUS.md")
            print("   This is OK if Process Status table hasn't been added yet")
            return process_items

        # Find table header
        content_after = self.content[match.end() :]
        table_pattern = r"\|\s*Process Name\s*\|\s*Strategy\s*\|\s*Type\s*\|\s*Status\s*\|\s*Extra Args\s*\|\s*Last Run\s*\|\s*Next Run\s*\|\s*Owner\s*\|"
        table_match = re.search(table_pattern, content_after)

        if not table_match:
            print("⚠️  WARNING: Process Status table format not found")
            return process_items

        # Extract table rows
        lines = content_after[table_match.end() :].split("\n")
        rows_parsed = 0
        rows_skipped = 0

        for line in lines[:30]:  # Limit to reasonable number
            if not line.strip() or not line.startswith("|"):
                continue

            parts = [p.strip() for p in line.split("|")]
            if len(parts) < 9:
                rows_skipped += 1
                continue

            process_name = parts[1]
            strategy = parts[2]
            process_type = parts[3]
            status = parts[4]
            extra_args = parts[5]
            last_run = parts[6]
            next_run = parts[7]
            owner = parts[8]

            # Skip header row
            if process_name == "Process Name" or "---" in process_name:
                continue

            # Handle multiple strategies (comma-separated)
            strategies = []
            if "," in strategy:
                strategies = [s.strip() for s in strategy.split(",")]
            else:
                strategies = [strategy.strip()]

            # Normalize strategy names to match milestone names
            normalized_strategies = []
            for s in strategies:
                # Map to milestone name format
                if s == "Delta-One ML":
                    normalized_strategies.append("ML Delta-One")
                elif s == "Crypto Options":
                    normalized_strategies.append("Crypto Options")
                elif s == "TradFi Options":
                    normalized_strategies.append("TradFi Options")
                elif s == "DeFi":
                    normalized_strategies.append("DeFi")
                elif s == "TradFi":
                    normalized_strategies.append("TradFi")
                elif s == "Sports Betting":
                    normalized_strategies.append("Sports Betting")
                else:
                    # Default: use as-is
                    normalized_strategies.append(s)

            strategies = normalized_strategies

            process_items.append(
                {
                    "process_name": process_name,
                    "strategies": strategies,
                    "process_type": process_type,
                    "status": status,
                    "extra_args": extra_args,
                    "last_run": last_run if last_run != "N/A" else None,
                    "next_run": next_run if next_run != "N/A" else None,
                    "owner": owner,
                }
            )
            rows_parsed += 1

        if rows_parsed == 0:
            print("⚠️  WARNING: No process status rows parsed")
        elif rows_skipped > 0:
            print(
                f"⚠️  WARNING: {rows_skipped} process status row(s) skipped due to formatting issues"
            )
        else:
            print(f"   Found {rows_parsed} process status row(s)")

        return process_items


class ClickUpImporter:
    """Main importer class"""

    def __init__(
        self,
        api_token: str,
        list_id: str,
        dry_run: bool = False,
        sprint_start_date: Optional[str] = None,
        clean_orphaned: bool = False,
        service_name: Optional[str] = None,
    ):
        self.client = ClickUpClient(api_token)
        self.list_id = list_id
        self.dry_run = dry_run
        self.clean_orphaned = clean_orphaned
        self.task_map = {}  # Map task names to task IDs
        self.custom_fields = {}  # Map field names to field IDs
        self.user_map = {}  # Map usernames to user IDs
        self.existing_tasks = {}  # Cache of existing tasks by name (for idempotency)
        self.all_tasks_from_status_md = (
            set()
        )  # Track all task names from STATUS.md (for orphan detection)

        # Detect service name from script path or use provided
        if service_name:
            self.service_name = service_name
        else:
            # Extract from script path: {service}/scripts/clickup_import.py -> {service}
            script_path = Path(__file__)
            self.service_name = script_path.parent.parent.name

        self.service_tag = (
            self.service_name
        )  # e.g., "instruments-service" or "market-tick-data-handler"

        # Calculate week dates based on sprint start
        # Default: Nov 9, 2025 (actual start date), Week 1 ends Nov 16
        if sprint_start_date:
            self.sprint_start = datetime.strptime(sprint_start_date, "%Y-%m-%d")
        else:
            self.sprint_start = datetime(2025, 11, 9)  # Nov 9, 2025 - actual start date

        # Calculate week end dates (each week is 7 days)
        self.week_dates = {}
        for week_num in range(1, 11):
            week_start = self.sprint_start + timedelta(days=(week_num - 1) * 7)
            week_end = week_start + timedelta(days=6)  # Week ends 6 days after start
            self.week_dates[f"Week {week_num}"] = week_end

        # Week ranges (e.g., Week 5-6 = end of Week 6)
        self.week_dates["Week 5-6"] = self.week_dates["Week 6"]
        self.week_dates["Week 7-8"] = self.week_dates["Week 8"]

        # Assignee mapping (username -> will be resolved to user ID)
        self.assignee_map = {
            "Ikenna": "",  # Will be resolved from instruments_config or API
            "Harsh": "",  # Will be resolved from instruments_config or API
            "Femi": "",  # Will be resolved from instruments_config or API
            "Daniel": "",  # Will be resolved from instruments_config or API
            "Carlos": "",  # Will be resolved from instruments_config or API
        }

    def resolve_user_ids(self):
        """Resolve username to user IDs from instruments_config or ClickUp team"""
        # Get user IDs from instruments_config (which reads from .env via Pydantic settings)
        if instruments_config.clickup_user_id_ikenna:
            self.assignee_map["Ikenna"] = instruments_config.clickup_user_id_ikenna
        if instruments_config.clickup_user_id_harsh:
            self.assignee_map["Harsh"] = instruments_config.clickup_user_id_harsh
        if instruments_config.clickup_user_id_femi:
            self.assignee_map["Femi"] = instruments_config.clickup_user_id_femi
        if instruments_config.clickup_user_id_daniel:
            self.assignee_map["Daniel"] = instruments_config.clickup_user_id_daniel
        # Carlos user ID is not in instruments_config, will be resolved from API if needed

        # If not found in instruments_config, try API (for dry run, use fake IDs)
        if self.dry_run:
            if not self.assignee_map.get("Ikenna"):
                self.assignee_map["Ikenna"] = "dry-run-user-1"
            if not self.assignee_map.get("Harsh"):
                self.assignee_map["Harsh"] = "dry-run-user-2"
            if not self.assignee_map.get("Femi"):
                self.assignee_map["Femi"] = "dry-run-user-3"
            if not self.assignee_map.get("Daniel"):
                self.assignee_map["Daniel"] = "dry-run-user-4"
            if not self.assignee_map.get("Carlos"):
                self.assignee_map["Carlos"] = "dry-run-user-5"
            print("🔍 [DRY RUN] Using user IDs from instruments_config or fake IDs")
            return

        # If still not found, try API
        if (
            not self.assignee_map.get("Ikenna")
            or not self.assignee_map.get("Harsh")
            or not self.assignee_map.get("Femi")
        ):
            try:
                # Get team members
                teams = self.client._request("GET", "/team")
                for team in teams.get("teams", []):
                    for member in team.get("members", []):
                        user = member.get("user", {})
                        username = user.get("username", "").lower()
                        email = user.get("email", "").lower()  # Also check email
                        full_name = user.get("name", "").lower()  # Full name field
                        user_id = user.get("id")

                        # Check username, email, and full name for "ikenna" or "igboaka"
                        if (
                            "ikenna" in username
                            or "igboaka" in username
                            or "ikenna" in email
                            or "igboaka" in email
                            or "ikenna" in full_name
                            or "igboaka" in full_name
                        ) and not self.assignee_map.get("Ikenna"):
                            self.assignee_map["Ikenna"] = user_id
                        elif (
                            "harsh" in username or "harsh" in email
                        ) and not self.assignee_map.get("Harsh"):
                            self.assignee_map["Harsh"] = user_id
                        elif (
                            "femi" in username
                            or "femi" in email
                            or "datadodo" in email
                            or "femi" in full_name
                        ) and not self.assignee_map.get("Femi"):
                            self.assignee_map["Femi"] = user_id
            except Exception as e:
                print(f"⚠️  Could not resolve user IDs from API: {e}")

        # Diagnostic: Show which assignee IDs are loaded
        print("\n📋 Assignee User IDs:")
        for name in ["Ikenna", "Harsh", "Femi", "Daniel", "Carlos"]:
            user_id = self.assignee_map.get(name)
            if user_id:
                print(f"   ✅ {name}: {user_id[:8]}...")
            else:
                print(f"   ❌ {name}: NOT FOUND")

        if (
            not self.assignee_map.get("Ikenna")
            or not self.assignee_map.get("Harsh")
            or not self.assignee_map.get("Femi")
        ):
            print("\n⚠️  WARNING: Some assignee user IDs are missing!")
            print("   Tasks for missing users will be created without assignees.")
            print("   To fix this:")
            print("   1. Run: python scripts/get_clickup_user_ids.py")
            print("   2. Add the user IDs to .env file (will be loaded by settings.py):")
            print("      clickup_user_id_ikenna=YOUR_ID")
            print("      clickup_user_id_harsh=YOUR_ID")
            print("      clickup_user_id_femi=YOUR_ID")
            print()

    def get_week_due_date(self, week_str: str) -> Optional[int]:
        """Convert week string (e.g., 'Week 5-6') to Unix timestamp"""
        if not week_str or week_str == "TBD":
            return None

        week_end = self.week_dates.get(week_str)
        if week_end:
            return int(week_end.timestamp() * 1000)

        return None

    def get_strategy_option_ids(self, strategy_names: List[str]) -> List[str]:
        """Get option IDs for strategy names from the Strategy custom field"""
        if not strategy_names or not self.custom_fields.get("Strategy"):
            return []

        option_ids = []
        try:
            field_id = self.custom_fields["Strategy"]
            # Get field details to find option IDs
            fields = self.client.get_custom_fields(self.list_id)
            for field in fields:
                if field.get("id") == field_id:
                    options = field.get("type_config", {}).get("options", [])
                    # Map strategy names to option IDs
                    strategy_map = {
                        "Delta-One ML": "Delta-One ML",
                        "DeFi": "DeFi",
                        "Options": "Options",
                        "TradFi": "TradFi",
                        "Sports Betting": "Sports Betting",
                    }
                    for strategy_name in strategy_names:
                        # Find matching option
                        for option in options:
                            option_name = option.get("name", "")
                            if (
                                strategy_map.get(strategy_name) == option_name
                                or strategy_name in option_name
                            ):
                                option_id = option.get("id")
                                if option_id:
                                    option_ids.append(option_id)
                                break
                    break
        except Exception as e:
            print(f"⚠️  Could not get strategy option IDs: {e}")

        return option_ids

    def get_week_option_id(self, week_string: str) -> Optional[str]:
        """Get the option ID for a week string from the Week custom field"""
        if not week_string or not self.custom_fields.get("Week"):
            return None

        # Get the custom field definition to find option IDs
        try:
            field_id = self.custom_fields["Week"]
            # Get field details to find option IDs
            fields = self.client.get_custom_fields(self.list_id)
            for field in fields:
                if field.get("id") == field_id:
                    options = field.get("type_config", {}).get("options", [])
                    # Find matching option
                    for option in options:
                        if option.get("name") == week_string:
                            return option.get("id")  # Return option ID
                    # If no exact match, try partial match
                    for option in options:
                        if week_string in option.get("name", ""):
                            return option.get("id")
                    break
        except Exception as e:
            print(f"⚠️  Could not get week option ID: {e}")

        return None

    def ensure_custom_fields(self):
        """Ensure custom fields exist, create if needed"""
        if self.dry_run:
            print("🔍 [DRY RUN] Would check/create custom fields")
            return

        try:
            existing_fields = self.client.get_custom_fields(self.list_id)
            for field in existing_fields:
                field_name = field.get("name", "")
                field_id = field.get("id", "")
                if field_name and field_id:
                    self.custom_fields[field_name] = field_id
            if self.custom_fields:
                print(f"   Found {len(self.custom_fields)} existing custom field(s)")
                # List existing fields for reference
                for name in sorted(self.custom_fields.keys()):
                    print(f"      - {name}")
        except Exception as e:
            print(f"⚠️  Could not load existing custom fields: {e}")
            # Continue anyway - will try to create fields

        # Define custom fields we need
        fields_to_create = [
            {"name": "Coverage %", "type": "number", "type_config": {"precision": 2}},
            {
                "name": "Test Coverage %",
                "type": "number",
                "type_config": {"precision": 2},
            },
            {
                "name": "DRY Compliance %",
                "type": "number",
                "type_config": {"precision": 2},
            },
            {
                "name": "Week",
                "type": "drop_down",
                "type_config": {
                    "options": [
                        {"name": "Week 1-2", "color": "#2196F3"},
                        {"name": "Week 3-4", "color": "#4CAF50"},
                        {"name": "Week 5-6", "color": "#FF9800"},
                        {"name": "Week 7-8", "color": "#F44336"},
                        {"name": "Week 9-10", "color": "#9C27B0"},
                        {"name": "TBD", "color": "#9E9E9E"},
                    ]
                },
            },
            {
                "name": "Strategy",
                "type": "multi_select",
                "type_config": {
                    "options": [
                        {"name": "Delta-One ML", "color": "#2196F3"},
                        {"name": "DeFi", "color": "#4CAF50"},
                        {"name": "Options", "color": "#FF9800"},
                        {"name": "TradFi", "color": "#F44336"},
                        {"name": "Sports Betting", "color": "#FF6B6B"},
                    ]
                },
            },
        ]

        for field_data in fields_to_create:
            if field_data["name"] not in self.custom_fields:
                try:
                    result = self.client.create_custom_field(self.list_id, field_data)
                    if result and result.get("id"):
                        self.custom_fields[field_data["name"]] = result["id"]
                        print(f"✅ Created custom field: {field_data['name']}")
                    else:
                        print(f"⚠️  Custom field '{field_data['name']}' created but no ID returned")
                except Exception as e:
                    error_msg = str(e)
                    # Check if it's a 400 error - might be format issue or field already exists
                    if "400" in error_msg:
                        # Check if field already exists (this is OK - script will use existing field)
                        if field_data["name"] in self.custom_fields:
                            print(
                                f"✅ Custom field '{field_data['name']}' already exists (ID: {self.custom_fields[field_data['name']]})"
                            )
                        else:
                            print(
                                f"⚠️  Could not create custom field '{field_data['name']}': {error_msg}"
                            )
                            print(
                                "   💡 This is usually OK - field might already exist with different format"
                            )
                            print("   💡 Script will continue and use existing field if found")
                            print(
                                "   💡 If tasks fail due to missing field, create manually in ClickUp UI"
                            )
                    else:
                        print(f"⚠️  Could not create custom field {field_data['name']}: {e}")

    def create_dependency(self, task_id: str, depends_on_task_id: str) -> bool:
        """Create a task dependency"""
        if self.dry_run:
            print(f"🔍 [DRY RUN] Would link dependency: {task_id} depends on {depends_on_task_id}")
            return True

        try:
            payload = {
                "depends_on": depends_on_task_id,
                "type": 1,  # 1 = waiting on, 2 = blocking
            }
            self.client._request("POST", f"/task/{task_id}/dependency", json=payload)
            print(f"✅ Linked dependency: {task_id} depends on {depends_on_task_id}")
            return True
        except Exception as e:
            print(f"⚠️  Could not link dependency: {e}")
            return False

    def update_task(self, task_id: str, updates: Dict) -> bool:
        """Update an existing task"""
        if self.dry_run:
            print(f"🔍 [DRY RUN] Would update task {task_id}: {updates}")
            return True

        try:
            self.client.update_task(task_id, updates)
            print(f"✅ Updated task {task_id}")
            return True
        except Exception as e:
            print(f"⚠️  Could not update task: {e}")
            return False

    def get_team_members(self) -> List[Dict]:
        """Get team members for assignee mapping"""
        if self.dry_run:
            return []

        try:
            # Get workspace ID from list
            list_info = self.client.get_list(self.list_id)
            workspace_id = list_info.get("workspace", {}).get("id")
            if not workspace_id:
                return []

            # Get team members
            teams = self.client._request("GET", "/team")
            members = []
            for team in teams.get("teams", []):
                if team.get("id") == workspace_id:
                    members = team.get("members", [])
                    break
            return members
        except Exception as e:
            print(f"⚠️  Could not get team members: {e}")
            return []

    def normalize_task_name(self, task_name: str) -> str:
        """Normalize task name for matching (handle truncation, backticks, markdown formatting, etc.)"""
        if not task_name:
            return ""
        # Clean task name - remove or replace problematic characters
        normalized = task_name.replace("`", "'")
        # Strip markdown bold formatting (**text** -> text)
        normalized = re.sub(r"\*\*(.+?)\*\*", r"\1", normalized)
        # Strip markdown italic formatting (*text* -> text, but be careful not to strip asterisks in the middle)
        normalized = re.sub(r"(?<!\*)\*([^*]+?)\*(?!\*)", r"\1", normalized)
        # Truncate to 100 chars if needed (same as ClickUp limit)
        if len(normalized) > 100:
            normalized = normalized[:97] + "..."
        return normalized

    def find_or_create_strategy_parent(
        self,
        parent_name: str,
        tags: List[str],
        strategies: List[str],
        due_date: Optional[int] = None,
    ) -> Optional[str]:
        """
        Find existing strategy milestone parent task or create if it doesn't exist.

        Strategy milestone parents are shared across services, so we check if they
        already exist before creating.

        Args:
            parent_name: Name of the strategy milestone (e.g., "ML Delta-One Strategy Backtest")
            tags: Tags for the parent task (should NOT include service tag)
            strategies: Strategy names for custom field
            due_date: Optional due date timestamp

        Returns:
            Task ID of the parent task, or None if creation failed
        """
        # Check if parent already exists
        existing_parent_id = self.existing_tasks.get(parent_name)

        if existing_parent_id:
            print(f"   ✅ Found existing strategy parent: {parent_name}")
            # Also add to task_map for consistency (used by data catalogue lookup)
            self.task_map[parent_name] = existing_parent_id
            return existing_parent_id

        # Parent doesn't exist - create it
        # Note: Parent tasks do NOT have service tag (they're shared)
        parent_task = {
            "name": parent_name,
            "status": "to do",
            "tags": tags,  # Should NOT include service tag
            "strategies": strategies,
            "due_date": due_date,
        }

        print(f"   🆕 Creating new strategy parent: {parent_name}")
        parent_id = self.create_task(parent_task)

        if parent_id:
            # Add to existing_tasks cache so other services can find it
            self.existing_tasks[parent_name] = parent_id
            self.task_map[parent_name] = parent_id

        return parent_id

    def create_task(self, task_data: Dict, parent_id: Optional[str] = None) -> Optional[str]:
        """Create a task and return its ID"""
        # Convert week string to actual date if needed
        if isinstance(task_data.get("due_date"), str) and "Week" in task_data["due_date"]:
            task_data["due_date"] = self.get_week_due_date(task_data["due_date"])

        # Determine assignee based on task name (if not already set)
        if "assignees" not in task_data or not task_data.get("assignees"):
            assignee_id = None
            task_name = task_data.get("name", "")

            # Infrastructure tasks -> Femi
            if (
                "Daily Backfill" in task_name
                or "backfill" in task_name.lower()
                or "VM" in task_name
                or "deployment" in task_name.lower()
                or "scheduler" in task_name.lower()
                or "infrastructure" in task_name.lower()
                or "unified-trading-deployment" in task_name.lower()
                or "Cloud Scheduler" in task_name
                or "one-off" in task_name.lower()
                or "Scheduler Running" in task_name
                or "VM Running" in task_name
            ):
                assignee_id = self.assignee_map.get("Femi")
                if not assignee_id:
                    print(
                        f"   ⚠️  Femi user ID not found - task '{task_name}' will be created without assignee"
                    )
            # Databento API Access -> Ikenna (credentials needed)
            elif "Databento API Access" in task_name:
                assignee_id = self.assignee_map.get("Ikenna")
            # Databento Integration/TradFi -> Ikenna (implementation)
            # Note: DeFi and TradFi are assigned to Ikenna (all completed tasks should be Ikenna per user request)
            elif (
                "Databento" in task_name or "TradFi" in task_name
            ) and "API Access" not in task_name:
                assignee_id = self.assignee_map.get("Ikenna")
            # DeFi -> Ikenna (all completed tasks should be Ikenna per user request)
            elif "DeFi" in task_name:
                assignee_id = self.assignee_map.get("Ikenna")
            else:
                # Default: Ikenna for instruments-service tasks
                assignee_id = self.assignee_map.get("Ikenna")

            if assignee_id:
                task_data["assignees"] = [assignee_id]

        if self.dry_run:
            indent = "  " if parent_id else ""
            parent_info = f" (parent: {parent_id})" if parent_id else ""

            # Build detail string
            details = []
            if task_data.get("priority"):
                priority_map = {1: "Urgent", 2: "High", 3: "Normal", 4: "Low"}
                details.append(
                    f"priority={priority_map.get(task_data['priority'], task_data['priority'])}"
                )
            if task_data.get("due_date"):
                if isinstance(task_data["due_date"], int):
                    due_date_str = datetime.fromtimestamp(task_data["due_date"] / 1000).strftime(
                        "%Y-%m-%d"
                    )
                    details.append(f"due_date={due_date_str}")
                elif isinstance(task_data["due_date"], str) and "Week" in task_data["due_date"]:
                    # Week string will be converted, show it
                    details.append(f"due_date={task_data['due_date']}")
            if task_data.get("week"):
                details.append(f"week={task_data['week']}")
            if task_data.get("tags"):
                details.append(f"tags={', '.join(task_data['tags'][:3])}")  # Show first 3 tags
            if task_data.get("assignees"):
                assignee_names = []
                for aid in task_data["assignees"]:
                    # Map user IDs to names for display
                    if aid == self.assignee_map.get("Ikenna"):
                        assignee_names.append("Ikenna")
                    elif aid == self.assignee_map.get("Harsh"):
                        assignee_names.append("Harsh")
                    elif aid == self.assignee_map.get("Femi"):
                        assignee_names.append("Femi")
                    elif aid == self.assignee_map.get("Daniel"):
                        assignee_names.append("Daniel")
                    elif aid == self.assignee_map.get("Carlos"):
                        assignee_names.append("Carlos")
                    else:
                        assignee_names.append(str(aid)[:8])  # Show first 8 chars of ID
                if assignee_names:
                    details.append(f"assignee={', '.join(assignee_names)}")

            detail_str = f" [{', '.join(details)}]" if details else ""

            print(
                f"🔍 [DRY RUN] Would create task: {indent}{task_data.get('name', 'Unknown')}{parent_info}{detail_str}"
            )
            # In dry run, still track tasks for dependency mapping
            fake_id = f"dry-run-{len(self.task_map)}"
            self.task_map[task_data["name"]] = fake_id
            return fake_id

        try:
            # Check if task already exists (idempotency)
            # Normalize task name for matching (handle truncation)
            original_task_name = task_data.get("name", "")
            normalized_task_name = self.normalize_task_name(original_task_name)

            # Check both original and normalized names
            existing_task_id = self.existing_tasks.get(
                original_task_name
            ) or self.existing_tasks.get(normalized_task_name)

            # Also check if any existing task name matches (for truncated names)
            if not existing_task_id:
                for existing_name, existing_id in self.existing_tasks.items():
                    normalized_existing = self.normalize_task_name(existing_name)
                    if normalized_existing == normalized_task_name:
                        existing_task_id = existing_id
                        break

            if existing_task_id:
                # Task exists - update it instead of creating duplicate
                print(
                    f"🔄 Task '{normalized_task_name}' already exists (ID: {existing_task_id}), updating..."
                )

                # Prepare update payload (only include fields that should be updated)
                update_payload = {}

                # Update description if provided
                if task_data.get("notes"):
                    update_payload["description"] = task_data.get("notes")

                # Update status if provided
                if task_data.get("status"):
                    update_payload["status"] = task_data.get("status")

                # Update tags if provided
                if task_data.get("tags"):
                    update_payload["tags"] = task_data.get("tags")

                # Update assignees if provided
                if "assignees" in task_data and task_data.get("assignees"):
                    update_payload["assignees"] = task_data["assignees"]

                # Update priority if provided
                if "priority" in task_data:
                    update_payload["priority"] = task_data["priority"]

                # Update due date if provided
                if task_data.get("due_date"):
                    if isinstance(task_data["due_date"], int):
                        update_payload["due_date"] = task_data["due_date"]
                    elif isinstance(task_data["due_date"], str) and "Week" in task_data["due_date"]:
                        week_date = self.get_week_due_date(task_data["due_date"])
                        if week_date:
                            update_payload["due_date"] = week_date

                # Update parent if provided
                if parent_id:
                    update_payload["parent"] = parent_id

                # Update custom fields
                custom_fields = []
                if "week" in task_data and task_data["week"]:
                    week_value = task_data["week"]
                    if self.custom_fields.get("Week"):
                        custom_fields.append(
                            {"id": self.custom_fields["Week"], "value": week_value}
                        )

                if "strategy" in task_data and task_data["strategy"]:
                    strategy_value = task_data["strategy"]
                    if self.custom_fields.get("Strategy"):
                        custom_fields.append(
                            {
                                "id": self.custom_fields["Strategy"],
                                "value": strategy_value,
                            }
                        )

                if custom_fields:
                    update_payload["custom_fields"] = custom_fields

                # Perform update
                if update_payload:
                    self.client.update_task(existing_task_id, update_payload)
                    print(f"✅ Updated task: {task_name}")

                # Track in task_map for dependency linking
                self.task_map[task_name] = existing_task_id
                return existing_task_id

            # Task doesn't exist - create new one
            # Prepare task payload
            # ClickUp task name limit is 100 characters - truncate if needed
            original_name = task_data["name"]
            task_name = self.normalize_task_name(original_name)

            if len(original_name) > 100:
                print(
                    f"⚠️  Task name too long ({len(original_name)} chars), truncating to 100 chars"
                )

            payload = {
                "name": task_name,
                "description": task_data.get("notes", ""),
                "status": task_data.get("status", "to do"),
                "tags": task_data.get("tags", []),
            }

            # Add parent if this is a subtask
            if parent_id:
                payload["parent"] = parent_id

            # If name was truncated, add full name to description
            if len(original_name) > 100:
                full_name_note = f"**Full task name:** {original_name}\n\n"
                payload["description"] = full_name_note + (payload.get("description") or "")

            # Add assignees if provided
            if "assignees" in task_data and task_data["assignees"]:
                payload["assignees"] = task_data["assignees"]

            # Add priority if provided (1=Urgent, 2=High, 3=Normal, 4=Low)
            if "priority" in task_data:
                payload["priority"] = task_data["priority"]

            # Add due_date if provided (for milestones and tasks with dates)
            if task_data.get("due_date"):
                if isinstance(task_data["due_date"], int):
                    # Already a timestamp
                    payload["due_date"] = task_data["due_date"]
                elif isinstance(task_data["due_date"], str) and "Week" in task_data["due_date"]:
                    # Convert week string to actual date
                    week_date = self.get_week_due_date(task_data["due_date"])
                    if week_date:
                        payload["due_date"] = week_date

            # Add start_date if provided
            if task_data.get("start_date"):
                payload["start_date"] = task_data["start_date"]

            # Add custom fields
            custom_fields = []

            # Week custom field (for milestones only)
            if "week" in task_data and task_data["week"]:
                week_value = task_data["week"]
                if self.custom_fields.get("Week"):
                    # For dropdown fields, we need the option ID, not the string
                    # Only set for milestones (not subtasks) to avoid errors
                    if not parent_id:  # Only for top-level tasks (milestones)
                        week_option_id = self.get_week_option_id(week_value)
                        if week_option_id:
                            custom_fields.append(
                                {
                                    "id": self.custom_fields["Week"],
                                    "value": week_option_id,  # Use option ID, not string
                                }
                            )
                        else:
                            # If we can't find the option ID, skip setting it (don't break subtasks)
                            print(
                                f"   ⚠️  Could not find option ID for week '{week_value}', skipping custom field"
                            )

            # Strategy custom field (for milestones only)
            if "strategies" in task_data and task_data["strategies"]:
                strategies = task_data["strategies"]
                if self.custom_fields.get("Strategy") and not parent_id:  # Only for milestones
                    strategy_option_ids = self.get_strategy_option_ids(strategies)
                    if strategy_option_ids:
                        custom_fields.append(
                            {
                                "id": self.custom_fields["Strategy"],
                                "value": strategy_option_ids,  # Multi-select: array of option IDs
                            }
                        )

            if custom_fields:
                payload["custom_fields"] = custom_fields

            # Create task
            result = self.client.create_task(self.list_id, payload)
            task_id = result.get("id") if result else None

            if task_id:
                print(f"✅ Created task: {task_name} (ID: {task_id})")
                # Use original name for mapping (for dependency linking)
                self.task_map[task_data["name"]] = task_id
                # Also add to existing_tasks cache for future runs (use truncated name)
                self.existing_tasks[task_name] = task_id
                return task_id
            else:
                print(f"⚠️  Task created but no ID returned: {task_name}")
                return None

        except Exception as e:
            error_msg = str(e)
            # Provide more helpful error messages
            if "400" in error_msg:
                print(f"❌ Error creating task {task_data.get('name', 'Unknown')}: {error_msg}")
                if parent_id:
                    print(f"   💡 This is a subtask (parent: {parent_id})")
                    print(f"   💡 Payload includes: parent={parent_id}, list_id={self.list_id}")
                if len(task_data.get("name", "")) > 100:
                    print(
                        f"   💡 Task name was truncated from {len(task_data.get('name', ''))} to 100 chars"
                    )
                # Check for common issues
                if "parent" in error_msg.lower():
                    print(f"   💡 Parent ID format might be invalid: {parent_id}")
                    print("   💡 Try checking if parent task exists in ClickUp")
                if "name" in error_msg.lower() or "title" in error_msg.lower():
                    print("   💡 Task name might contain invalid characters")
                # Debug: print payload keys (not values for security)
                if parent_id:
                    print(f"   💡 Payload keys: {list(payload.keys())}")
            else:
                print(f"❌ Error creating task {task_data.get('name', 'Unknown')}: {e}")
            return None

    def import_from_status_md(self, status_md_path: Path):
        """Import tasks from STATUS.md"""
        parser = StatusMdParser(status_md_path, service_tag=self.service_tag)

        print("📋 Parsing STATUS.md...")

        # Resolve user IDs first
        print("👤 Resolving assignees...")
        self.resolve_user_ids()
        if not self.dry_run:
            if self.assignee_map.get("Ikenna"):
                print(f"   ✅ Found Ikenna: {self.assignee_map['Ikenna']}")
            else:
                print("   ⚠️  Ikenna user ID not found - check .env file (loaded by settings.py)")
            if self.assignee_map.get("Harsh"):
                print(f"   ✅ Found Harsh: {self.assignee_map['Harsh']}")
            else:
                print("   ⚠️  Harsh user ID not found - check .env file (loaded by settings.py)")
            if self.assignee_map.get("Femi"):
                print(f"   ✅ Found Femi: {self.assignee_map['Femi']}")
            else:
                print("   ⚠️  Femi user ID not found - check .env file (loaded by settings.py)")
            if self.assignee_map.get("Daniel"):
                print(f"   ✅ Found Daniel: {self.assignee_map['Daniel']}")
            else:
                print("   ⚠️  Daniel user ID not found - check .env file (loaded by settings.py)")
            if self.assignee_map.get("Carlos"):
                print(f"   ✅ Found Carlos: {self.assignee_map['Carlos']}")

        # Print service information
        print(f"📦 Service: {self.service_name}")
        print(f"🏷️  Service tag: {self.service_tag}")

        # Ensure custom fields exist
        print("🔧 Setting up custom fields...")
        self.ensure_custom_fields()

        # Load existing tasks for idempotency (check for duplicates)
        if not self.dry_run:
            print("🔍 Loading existing tasks from ClickUp...")
            existing_tasks_list = self.client.get_tasks(self.list_id)
            for task in existing_tasks_list:
                task_name = task.get("name", "")
                task_id = task.get("id", "")
                if task_name and task_id:
                    # Store both original and normalized names for matching
                    self.existing_tasks[task_name] = task_id
                    normalized_name = self.normalize_task_name(task_name)
                    if normalized_name != task_name:
                        self.existing_tasks[normalized_name] = task_id
            if self.existing_tasks:
                # Count unique task IDs (not names, since we store both original and normalized)
                unique_task_ids = len(set(self.existing_tasks.values()))
                print(
                    f"   Found {unique_task_ids} existing task(s) - will update instead of duplicate"
                )

        # Parse milestones
        print("📊 Parsing milestones...")
        milestones = parser.parse_milestones()
        if len(milestones) == 0:
            print("   ⚠️  WARNING: No milestones found!")
            print("   Check STATUS.md formatting - need '## Timeline Tracking' section with table")
        else:
            print(f"   Found {len(milestones)} milestones")

        # Separate strategy milestones (cross-service) from service-specific milestones
        strategy_milestones = []
        service_milestones = []

        for milestone in milestones:
            if "Strategy" in milestone["name"]:
                strategy_milestones.append(milestone)
            else:
                service_milestones.append(milestone)

        # Create service-specific milestone tasks (non-strategy milestones)
        print("\n🎯 Creating service-specific milestone tasks...")
        for milestone in service_milestones:
            # Determine tags and strategy based on milestone name
            tags = [self.service_tag, "milestone"]
            strategies = []  # For Strategy custom field

            if "DeFi" in milestone["name"]:
                tags.append("defi")
                strategies.append("DeFi")
                milestone["priority"] = 2  # High priority (early dependency)
            elif "TradFi" in milestone["name"] or "Databento" in milestone["name"]:
                tags.append("tradfi")
                strategies.append("TradFi")
                milestone["priority"] = 3  # Normal priority (Week 7-8)
            elif "Options" in milestone["name"]:
                tags.append("options")
                strategies.append("Options")
            else:
                # Default: Delta-One ML for general instruments work
                strategies.append("Delta-One ML")

            milestone["tags"] = tags
            milestone["strategies"] = strategies  # Store for custom field
            task_id = self.create_task(milestone)
            if task_id:
                self.task_map[milestone["name"]] = task_id

        # Create strategy milestone parent tasks with service subtasks (Option A)
        print("\n🎯 Creating strategy milestone parent tasks...")
        for strategy_milestone in strategy_milestones:
            # Determine tags and strategy based on milestone name
            # Parent tasks do NOT have service tag (they're shared across services)
            tags = ["milestone", "strategy-milestone", "cross-service"]
            strategies = []

            if "ML Delta-One" in strategy_milestone["name"]:
                tags.append("ml-delta-one")
                strategies.append("Delta-One ML")
            elif "DeFi" in strategy_milestone["name"]:
                tags.append("defi")
                strategies.append("DeFi")
            elif "TradFi" in strategy_milestone["name"]:
                tags.append("tradfi")
                strategies.append("TradFi")
            elif "Crypto Options" in strategy_milestone["name"]:
                tags.append("crypto-options")
                strategies.append("Options")
            elif "TradFi Options" in strategy_milestone["name"]:
                tags.append("tradfi-options")
                strategies.append("Options")
            elif "Sports Betting" in strategy_milestone["name"]:
                tags.append("sports-betting")
                strategies.append("Sports Betting")

            # Find or create parent task (shared across services)
            parent_id = self.find_or_create_strategy_parent(
                parent_name=strategy_milestone["name"],
                tags=tags,
                strategies=strategies,
                due_date=strategy_milestone.get("due_date"),
            )

            if not parent_id:
                print(f"   ⚠️  Failed to create/find parent for {strategy_milestone['name']}")
                continue  # Skip creating subtasks if parent doesn't exist

            # Ensure parent is in task_map for data catalogue lookup
            if parent_id and strategy_milestone["name"] not in self.task_map:
                self.task_map[strategy_milestone["name"]] = parent_id
                print(f"   ✅ Added parent milestone '{strategy_milestone['name']}' to task_map")

            # Create service-specific subtasks for instruments-service
            # Distinguish between Code Complete, Batch Data Run, and Daily Backfill (for Live)
            # Make subtask names unique by including strategy name
            strategy_prefix = strategy_milestone["name"].replace(" Strategy", "").replace(" ", "-")
            service_subtasks = []

            if "ML Delta-One" in strategy_milestone["name"]:
                if "Backtest" in strategy_milestone["name"]:
                    # Code complete (CeFi + TradFi MVP)
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Code Complete",
                            "status": "complete",  # Code is complete
                            "notes": "CeFi crypto instruments (Tardis) and TradFi instruments (Databento) MVP code complete",
                            "assignees": (
                                [self.assignee_map.get("Ikenna")]
                                if self.assignee_map.get("Ikenna")
                                else None
                            ),
                        }
                    )
                    # Batch data not run yet
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Batch Data Run",
                            "status": "to do",  # Batch data hasn't been run
                            "notes": "Batch data backfill (Jan 1, 2020 - Today) needs to be run for backtest",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
                elif "Live" in strategy_milestone["name"]:
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Code Complete",
                            "status": "complete",
                            "notes": "Code complete for live trading",
                            "assignees": (
                                [self.assignee_map.get("Ikenna")]
                                if self.assignee_map.get("Ikenna")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Batch Data Run",
                            "status": "to do",
                            "notes": "Batch data backfill needed",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Daily Backfill Configured",
                            "status": "to do",
                            "notes": "Daily T+1 backfill scheduler needs to be configured",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
            elif "DeFi" in strategy_milestone["name"]:
                if "Backtest" in strategy_milestone["name"]:
                    # DeFi code NOT complete due to warnings and failed tests
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Code Complete",
                            "status": "in progress",  # NOT complete - warnings and failed tests
                            "notes": "DeFi instruments MVP structure exists but has warnings (Curve, Uniswap V4, The Graph issues) and failed tests. Not ready for backtest.",
                            "assignees": (
                                [self.assignee_map.get("Ikenna")]
                                if self.assignee_map.get("Ikenna")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Batch Data Run",
                            "status": "to do",
                            "notes": "Batch data backfill needed once code is complete",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
                elif "Live" in strategy_milestone["name"]:
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Code Complete",
                            "status": "in progress",
                            "notes": "DeFi code has warnings and failed tests",
                            "assignees": (
                                [self.assignee_map.get("Ikenna")]
                                if self.assignee_map.get("Ikenna")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Batch Data Run",
                            "status": "to do",
                            "notes": "Batch data backfill needed",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Daily Backfill Configured",
                            "status": "to do",
                            "notes": "Daily T+1 backfill scheduler needed",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
            elif (
                "TradFi" in strategy_milestone["name"]
                and "Options" not in strategy_milestone["name"]
            ):
                if "Backtest" in strategy_milestone["name"]:
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Code Complete",
                            "status": "complete",
                            "notes": "TradFi instruments (Databento) MVP code complete",
                            "assignees": (
                                [self.assignee_map.get("Ikenna")]
                                if self.assignee_map.get("Ikenna")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Batch Data Run",
                            "status": "to do",
                            "notes": "Batch data backfill (Jan 1, 2020 - Today) needed for backtest",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
                elif "Live" in strategy_milestone["name"]:
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Code Complete",
                            "status": "complete",
                            "notes": "TradFi code complete",
                            "assignees": (
                                [self.assignee_map.get("Ikenna")]
                                if self.assignee_map.get("Ikenna")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Batch Data Run",
                            "status": "to do",
                            "notes": "Batch data backfill needed",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Daily Backfill Configured",
                            "status": "to do",
                            "notes": "Daily T+1 backfill scheduler needed",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
            elif "Crypto Options" in strategy_milestone["name"]:
                if "Backtest" in strategy_milestone["name"]:
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Code Complete",
                            "status": "complete",
                            "notes": "Crypto options instruments (DERIBIT) MVP code complete",
                            "assignees": (
                                [self.assignee_map.get("Ikenna")]
                                if self.assignee_map.get("Ikenna")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Batch Data Run",
                            "status": "to do",
                            "notes": "Batch data backfill (Jan 1, 2020 - Today) needed for backtest",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
                elif "Live" in strategy_milestone["name"]:
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Code Complete",
                            "status": "complete",
                            "notes": "Crypto options code complete",
                            "assignees": (
                                [self.assignee_map.get("Ikenna")]
                                if self.assignee_map.get("Ikenna")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Batch Data Run",
                            "status": "to do",
                            "notes": "Batch data backfill needed",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Daily Backfill Configured",
                            "status": "to do",
                            "notes": "Daily T+1 backfill scheduler needed",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
            elif "TradFi Options" in strategy_milestone["name"]:
                if "Backtest" in strategy_milestone["name"]:
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Code Complete",
                            "status": "complete",
                            "notes": "TradFi options instruments (S&P 500 simple premium-based model) MVP code complete",
                            "assignees": (
                                [self.assignee_map.get("Ikenna")]
                                if self.assignee_map.get("Ikenna")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Batch Data Run",
                            "status": "to do",
                            "notes": "Batch data backfill (Jan 1, 2020 - Today) needed for backtest",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
                elif "Live" in strategy_milestone["name"]:
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Code Complete",
                            "status": "complete",
                            "notes": "TradFi options code complete",
                            "assignees": (
                                [self.assignee_map.get("Ikenna")]
                                if self.assignee_map.get("Ikenna")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Batch Data Run",
                            "status": "to do",
                            "notes": "Batch data backfill needed",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )
                    service_subtasks.append(
                        {
                            "name": f"{self.service_name} ({strategy_prefix}): Daily Backfill Configured",
                            "status": "to do",
                            "notes": "Daily T+1 backfill scheduler needed",
                            "assignees": (
                                [self.assignee_map.get("Femi")]
                                if self.assignee_map.get("Femi")
                                else None
                            ),
                        }
                    )

            # Create all service subtasks
            for subtask_data in service_subtasks:
                subtask_name = subtask_data["name"]
                # Check if this subtask already exists (might be created by other services)
                existing_subtask_id = self.existing_tasks.get(subtask_name)

                if existing_subtask_id:
                    # Update existing subtask to be under this parent
                    update_payload = {"parent": parent_id}
                    if self.update_task(existing_subtask_id, update_payload):
                        print(f"   🔄 Linked existing subtask '{subtask_name}' to parent")
                        self.task_map[subtask_name] = existing_subtask_id
                else:
                    # Create new subtask
                    service_subtask = {
                        "name": subtask_name,
                        "status": subtask_data["status"],
                        "tags": [self.service_tag] + tags + ["service-subtask"],
                        "notes": subtask_data["notes"],
                        "assignees": subtask_data.get("assignees"),
                    }
                    subtask_id = self.create_task(service_subtask, parent_id=parent_id)
                    if subtask_id:
                        self.task_map[subtask_name] = subtask_id

        # Parse and create data catalogue subtasks
        print("\n📊 Parsing data catalogue...")
        data_catalogue_items = parser.parse_data_catalogue()
        if len(data_catalogue_items) == 0:
            print("   ℹ️  No data catalogue items found (this is OK if table is empty)")
        else:
            print(f"   Found {len(data_catalogue_items)} data catalogue item(s)")

        print("\n📊 Creating data catalogue subtasks...")
        catalogue_subtask_count = 0
        for catalogue_item in data_catalogue_items:
            data_type = catalogue_item["data_type"]
            strategies = catalogue_item["strategies"]
            date_from = catalogue_item["date_from"]
            date_to = catalogue_item["date_to"]
            status = catalogue_item["status"]
            notes = catalogue_item["notes"]
            coverage_pct = catalogue_item.get("coverage_pct")

            # Map status to ClickUp status
            if "Complete" in status:
                clickup_status = "complete"
            elif "Partial" in status:
                clickup_status = "in progress"
            elif "Missing" in status:
                clickup_status = "to do"
            else:
                clickup_status = "to do"

            # Create subtask for each strategy
            for strategy in strategies:
                # Find matching strategy milestone parent (look for Backtest milestone)
                parent_milestone_name = f"{strategy} Strategy Backtest"
                # Normalize the lookup name to match stored names (which may have markdown formatting)
                normalized_lookup_name = self.normalize_task_name(parent_milestone_name)
                parent_id = None

                # Try exact match first
                parent_id = self.task_map.get(parent_milestone_name) or self.existing_tasks.get(
                    parent_milestone_name
                )

                # If not found, try normalized match (in case stored name has markdown)
                if not parent_id:
                    for stored_name, stored_id in self.task_map.items():
                        if self.normalize_task_name(stored_name) == normalized_lookup_name:
                            parent_id = stored_id
                            break
                    if not parent_id:
                        for stored_name, stored_id in self.existing_tasks.items():
                            if self.normalize_task_name(stored_name) == normalized_lookup_name:
                                parent_id = stored_id
                                break

                if not parent_id:
                    # Try Live milestone if Backtest doesn't exist
                    parent_milestone_name = f"{strategy} Strategy Live"
                    normalized_lookup_name = self.normalize_task_name(parent_milestone_name)
                    parent_id = self.task_map.get(parent_milestone_name) or self.existing_tasks.get(
                        parent_milestone_name
                    )

                    # If not found, try normalized match
                    if not parent_id:
                        for stored_name, stored_id in self.task_map.items():
                            if self.normalize_task_name(stored_name) == normalized_lookup_name:
                                parent_id = stored_id
                                break
                        if not parent_id:
                            for stored_name, stored_id in self.existing_tasks.items():
                                if self.normalize_task_name(stored_name) == normalized_lookup_name:
                                    parent_id = stored_id
                                    break

                if not parent_id:
                    # Debug: Show what's in task_map and existing_tasks
                    print(f"   ⚠️  Could not find parent milestone for strategy '{strategy}'")
                    print(
                        f"      Looking for: '{parent_milestone_name}' (normalized: '{normalized_lookup_name}')"
                    )
                    print(
                        f"      Available milestones in task_map: {[k for k in self.task_map.keys() if 'Strategy' in k][:5]}"
                    )
                    print(
                        f"      Available milestones in existing_tasks: {[k for k in self.existing_tasks.keys() if 'Strategy' in k][:5]}"
                    )
                    print("      Skipping data catalogue subtask")
                    continue

                # Create subtask name
                subtask_name = f"{self.service_name} ({strategy.replace(' ', '-')}): Data Catalogue - {data_type}"

                # Build description
                description_parts = []
                if date_from and date_to:
                    description_parts.append(f"**Date Range:** {date_from} to {date_to}")
                if notes:
                    description_parts.append(f"**Notes:** {notes}")
                description = "\n\n".join(description_parts) if description_parts else notes or ""

                # Create subtask
                catalogue_subtask = {
                    "name": subtask_name,
                    "status": clickup_status,
                    "tags": [self.service_tag, "data-catalogue", "deployment"],
                    "notes": description,
                    "assignees": (
                        [self.assignee_map.get("Femi")] if self.assignee_map.get("Femi") else None
                    ),
                }

                subtask_id = self.create_task(catalogue_subtask, parent_id=parent_id)
                if subtask_id:
                    # Update Coverage % custom field if needed
                    if coverage_pct is not None and self.custom_fields.get("Coverage %"):
                        update_payload = {
                            "custom_fields": [
                                {
                                    "id": self.custom_fields["Coverage %"],
                                    "value": coverage_pct,
                                }
                            ]
                        }
                        self.update_task(subtask_id, update_payload)

                    self.task_map[subtask_name] = subtask_id
                    catalogue_subtask_count += 1

        if catalogue_subtask_count > 0:
            print(f"   ✅ Created {catalogue_subtask_count} data catalogue subtask(s)")

        # Parse and create process status tasks
        print("\n⚙️  Parsing process status...")
        process_status_items = parser.parse_process_status()
        if len(process_status_items) == 0:
            print("   ℹ️  No process status items found (this is OK if table hasn't been added yet)")
        else:
            print(f"   Found {len(process_status_items)} process status item(s)")

        print("\n⚙️  Creating process status tasks...")
        process_task_count = 0
        for process_item in process_status_items:
            process_name = process_item["process_name"]
            strategies = process_item["strategies"]
            process_type = process_item["process_type"]
            status = process_item["status"]
            extra_args = process_item["extra_args"]
            last_run = process_item["last_run"]
            next_run = process_item["next_run"]
            owner = process_item["owner"]

            # Map status to ClickUp status
            if "Running" in status:
                clickup_status = "in progress"
            elif "Stopped" in status:
                clickup_status = "complete"  # Stopped = done/completed
            elif "Not Configured" in status or "Not Running" in status:
                clickup_status = "to do"
            else:
                clickup_status = "to do"

            # Build description
            description_parts = []
            description_parts.append(f"**Type:** {process_type}")
            description_parts.append(f"**Status:** {status}")
            if extra_args:
                description_parts.append(f"**Extra Args:** {extra_args}")
            if last_run:
                description_parts.append(f"**Last Run:** {last_run}")
            if next_run:
                description_parts.append(f"**Next Run:** {next_run}")
            description = "\n\n".join(description_parts)

            # Create task name
            task_name = f"{self.service_name}: Process Status - {process_name}"

            # Get strategy option IDs for multi-select Strategy custom field
            strategy_option_ids = []
            if strategies and self.custom_fields.get("Strategy"):
                strategy_option_ids = self.get_strategy_option_ids(strategies)

            # Create standalone task
            process_task = {
                "name": task_name,
                "status": clickup_status,
                "tags": [self.service_tag, "process-status", "deployment"],
                "notes": description,
                "assignees": (
                    [self.assignee_map.get(owner)]
                    if owner and self.assignee_map.get(owner)
                    else None
                ),
            }

            task_id = self.create_task(process_task)
            if task_id:
                # Update Strategy custom field if needed
                if strategy_option_ids and self.custom_fields.get("Strategy"):
                    update_payload = {
                        "custom_fields": [
                            {
                                "id": self.custom_fields["Strategy"],
                                "value": strategy_option_ids,  # Multi-select: array of option IDs
                            }
                        ]
                    }
                    self.update_task(task_id, update_payload)

                self.task_map[task_name] = task_id
                process_task_count += 1

        if process_task_count > 0:
            print(f"   ✅ Created {process_task_count} process status task(s)")

        # Parse and create completed milestones
        print("\n✅ Parsing completed milestones...")
        completed_milestones = parser.parse_completed_milestones()
        if len(completed_milestones) == 0:
            print("   ℹ️  No completed milestones found (this is OK if none exist)")
        else:
            print(f"   Found {len(completed_milestones)} completed milestone(s)")

        print("\n🎯 Creating completed milestone tasks...")
        for completed_milestone in completed_milestones:
            tags = [self.service_tag, "milestone", "complete"]
            strategies = []

            if "DeFi" in completed_milestone["name"]:
                tags.append("defi")
                strategies.append("DeFi")
            elif (
                "TradFi" in completed_milestone["name"]
                or "Databento" in completed_milestone["name"]
            ):
                tags.append("tradfi")
                strategies.append("TradFi")
            elif "Options" in completed_milestone["name"]:
                tags.append("options")
                strategies.append("Options")
            else:
                strategies.append("Delta-One ML")

            completed_milestone["tags"] = tags
            completed_milestone["strategies"] = strategies
            # All completed milestones are assigned to Ikenna (as per user request)
            completed_milestone["assignees"] = (
                [self.assignee_map.get("Ikenna")] if self.assignee_map.get("Ikenna") else None
            )
            task_id = self.create_task(completed_milestone)
            if task_id:
                self.task_map[completed_milestone["name"]] = task_id

        # Parse and create priority tasks
        print("\n📝 Parsing priority tasks...")
        priority_tasks = parser.parse_priority_tasks()
        if len(priority_tasks) == 0:
            print("   ⚠️  WARNING: No priority tasks found!")
            print(
                "   Check STATUS.md formatting - need '## Next Steps' > priority sections (High/Medium/Low)"
            )
        else:
            print(f"   Found {len(priority_tasks)} priority task(s)")

        # Create priority tasks
        print("\n🎯 Creating priority tasks...")
        task_count = 0
        subtask_count = 0

        for task_data in priority_tasks:
            # Determine assignee
            owner_name = task_data.get("owner", "").strip()
            assignee_id = None
            if owner_name:
                assignee_id = self.assignee_map.get(owner_name)
                if not assignee_id:
                    print(f"   ⚠️  Owner '{owner_name}' not found in assignee_map")
                    if owner_name == "Femi":
                        print("      💡 Add clickup_user_id_femi=... to .env file")
                        # Try auto-assignment based on task name as fallback
                        task_name = task_data.get("name", "")
                        if (
                            "Daily Backfill" in task_name
                            or "backfill" in task_name.lower()
                            or "VM" in task_name
                            or "deployment" in task_name.lower()
                            or "scheduler" in task_name.lower()
                            or "infrastructure" in task_name.lower()
                            or "unified-trading-deployment" in task_name.lower()
                            or "Cloud Scheduler" in task_name
                            or "one-off" in task_name.lower()
                            or "Scheduler Running" in task_name
                            or "VM Running" in task_name
                        ):
                            # Try to get Femi from assignee_map again (might have been resolved)
                            assignee_id = self.assignee_map.get("Femi")
                            if assignee_id:
                                print("      ✅ Found Femi via auto-assignment fallback")

            # Determine tags based on priority level
            tags = [self.service_tag, "task"]
            if task_data.get("priority_level") == "High":
                tags.append("high-priority")
            elif task_data.get("priority_level") == "Medium":
                tags.append("medium-priority")
            elif task_data.get("priority_level") == "Low":
                tags.append("low-priority")

            # Create parent task
            # Don't set assignees to None - let create_task handle auto-assignment if assignee_id is None
            parent_task_data = {
                "name": task_data["name"],
                "status": "to do",
                "tags": tags,
                "due_date": task_data.get("due_date"),
                "priority": task_data.get("priority", 3),
                "notes": f"Blocks: {task_data.get('blocks', 'None')}\nDependencies: {task_data.get('dependencies', 'None')}",
            }
            # Only add assignees if we have a valid ID (don't set to None - let create_task handle auto-assignment)
            if assignee_id:
                parent_task_data["assignees"] = [assignee_id]

            parent_id = self.create_task(parent_task_data)
            if parent_id:
                self.task_map[task_data["name"]] = parent_id
                task_count += 1

                # Create subtasks if any
                for subtask_name in task_data.get("subtasks", []):
                    subtask_task_data = {
                        "name": subtask_name,
                        "status": "to do",
                        "tags": tags + ["subtask"],
                    }
                    # Only add assignees if we have a valid ID
                    if assignee_id:
                        subtask_task_data["assignees"] = [assignee_id]
                    self.create_task(subtask_task_data, parent_id=parent_id)
                    subtask_count += 1

        if task_count > 0:
            print(f"   ✅ Created {task_count} priority task(s)")
        if subtask_count > 0:
            print(f"   ✅ Created {subtask_count} subtask(s)")

        # Parse in-progress tasks
        print("\n🔄 Parsing in-progress tasks...")
        in_progress = parser.parse_in_progress_tasks()
        if len(in_progress) == 0:
            print("   ℹ️  No in-progress tasks found (this is OK if none exist)")
        else:
            print(f"   Found {len(in_progress)} in-progress tasks")

        for task_data in in_progress:
            task_data["tags"] = [self.service_tag, "in-progress", "bug-fix"]
            self.create_task(task_data)

        # Parse and create dependency tasks (from Dependencies section)
        print("\n🔧 Parsing dependencies...")
        parsed_dependencies = parser.parse_dependencies()
        if len(parsed_dependencies) == 0:
            print("   ℹ️  No dependencies found (this is OK if none exist)")
        else:
            print(f"   Found {len(parsed_dependencies)} dependency/dependencies")

        print("\n🔧 Creating dependency tasks...")
        for dep_task in parsed_dependencies:
            if dep_task["name"] not in self.task_map:
                task_id = self.create_task(dep_task)
                if task_id:
                    self.task_map[dep_task["name"]] = task_id
            else:
                # Update existing dependency task status if it changed
                existing_id = self.task_map[dep_task["name"]]
                update_payload = {"status": dep_task["status"]}
                if self.update_task(existing_id, update_payload):
                    print(
                        f"   🔄 Updated dependency '{dep_task['name']}' status to '{dep_task['status']}'"
                    )

        # Link dependencies after all tasks are created
        print("\n🔗 Linking task dependencies...")
        dependency_map = {
            "DeFi Instrument Support": "unified-cloud-services Integration",
            "Databento Integration (TradFi)": "Databento API Access",
        }

        dependency_count = 0
        for task_name, depends_on_name in dependency_map.items():
            task_id = self.task_map.get(task_name)
            depends_on_id = self.task_map.get(depends_on_name)

            if task_id and depends_on_id:
                if self.create_dependency(task_id, depends_on_id):
                    dependency_count += 1
            else:
                if not task_id:
                    print(f"⚠️  Task '{task_name}' not found for dependency")
                    # Check if it's a completed milestone that should exist
                    if task_name in [cm["name"] for cm in completed_milestones]:
                        print(
                            f"   💡 Note: '{task_name}' is a completed milestone - ensure it was created"
                        )
                if not depends_on_id:
                    print(
                        f"⚠️  Dependency task '{depends_on_name}' not found - check if it's in Dependencies section of STATUS.md"
                    )

        # Track all task names from STATUS.md for orphan detection
        for milestone in milestones:
            self.all_tasks_from_status_md.add(milestone["name"])

        for completed_milestone in completed_milestones:
            self.all_tasks_from_status_md.add(completed_milestone["name"])

        for task_data in priority_tasks:
            self.all_tasks_from_status_md.add(task_data["name"])
            for subtask_name in task_data.get("subtasks", []):
                self.all_tasks_from_status_md.add(subtask_name)

        for task in in_progress:
            self.all_tasks_from_status_md.add(task["name"])

        for dep_task in parsed_dependencies:
            self.all_tasks_from_status_md.add(dep_task["name"])

        # Handle orphaned tasks (tasks in ClickUp but not in STATUS.md)
        if self.clean_orphaned and not self.dry_run:
            print("\n🧹 Cleaning orphaned tasks...")
            orphaned_count = 0
            for task_name, task_id in self.existing_tasks.items():
                if task_name not in self.all_tasks_from_status_md:
                    # Check if it's a task we manage (has our tags) - be conservative
                    # Only delete if it has our service tag
                    try:
                        task_info = self.client._request("GET", f"/task/{task_id}")
                        tags = [tag.get("name", "") for tag in task_info.get("tags", [])]
                        if self.service_tag in tags:
                            print(f"   🗑️  Deleting orphaned task: {task_name} (ID: {task_id})")
                            if self.client.delete_task(task_id):
                                orphaned_count += 1
                            else:
                                print("      ⚠️  Failed to delete")
                    except Exception as e:
                        print(f"      ⚠️  Could not check/delete task {task_name}: {e}")

            if orphaned_count > 0:
                print(f"\n   ✅ Deleted {orphaned_count} orphaned task(s)")
            else:
                print("   ✅ No orphaned tasks to delete")
        elif not self.dry_run:
            # Just warn about orphans, don't delete
            orphaned_tasks = []
            for task_name, task_id in self.existing_tasks.items():
                if task_name not in self.all_tasks_from_status_md:
                    orphaned_tasks.append((task_name, task_id))

            if orphaned_tasks:
                print(
                    f"\n⚠️  Found {len(orphaned_tasks)} orphaned task(s) (in ClickUp but not in STATUS.md):"
                )
                for task_name, task_id in orphaned_tasks[:10]:  # Show first 10
                    print(f"   - {task_name}")
                if len(orphaned_tasks) > 10:
                    print(f"   ... and {len(orphaned_tasks) - 10} more")
                print("\n   💡 These may have been renamed or removed from STATUS.md.")
                print(
                    "   💡 To delete them, run with --clean-orphaned flag (or delete manually in ClickUp)"
                )

        # Count tasks vs subtasks
        total_tasks = len(self.task_map)
        milestone_count = len(milestones)
        in_progress_count = len(in_progress)

        print("\n✅ Import complete!")
        print("📊 Summary:")
        print(f"   - Milestone tasks: {milestone_count}")
        print(f"   - Subtasks: {subtask_count}")
        print(f"   - In-progress tasks: {in_progress_count}")
        print(f"   - Dependencies linked: {dependency_count}")
        print(f"   - Total tasks: {total_tasks}")

        if not self.dry_run:
            print("\n📋 Task mapping (for reference):")
            print(json.dumps(self.task_map, indent=2))

        # Note about dependencies and timelines
        print("\n📝 Import Details:")
        if not self.dry_run:
            print(f"   ✅ Dependencies: {dependency_count} automatically linked via API")
            print("   ✅ Priorities: Automatically set (DeFi=High, TradFi=Normal)")
            print(
                f"   ✅ Due Dates: Automatically set (Week 5-6 = {self.week_dates['Week 5-6'].strftime('%Y-%m-%d')}, Week 7-8 = {self.week_dates['Week 7-8'].strftime('%Y-%m-%d')})"
            )
            print("   ✅ Assignees: Automatically assigned (Ikenna for all tasks)")
            print("   ✅ Tags: Automatically applied")
            print("   ✅ Custom Fields: Automatically created and populated")
        else:
            print(
                f"   ✅ Dependencies: Will be linked automatically ({len(dependency_map)} dependencies)"
            )
            print("   ✅ Priorities: Will be set automatically (DeFi=High, TradFi=Normal)")
            print(
                f"   ✅ Due Dates: Will be set automatically (Week 5-6 = {self.week_dates['Week 5-6'].strftime('%Y-%m-%d')}, Week 7-8 = {self.week_dates['Week 7-8'].strftime('%Y-%m-%d')})"
            )
            print(
                "   ✅ Assignees: Will be assigned automatically (Ikenna for code tasks, Femi for infrastructure/backfill)"
            )
            print("   ✅ Tags: Will be applied automatically")
            print("   ✅ Custom Fields: Will be created and populated automatically")

        print("\n📅 Week Date Mapping:")
        print(f"   Sprint Start: {self.sprint_start.strftime('%Y-%m-%d')} (Nov 7, 2025)")
        print(
            f"   Week 5-6 ends: {self.week_dates['Week 5-6'].strftime('%Y-%m-%d')} (Dec 19, 2025)"
        )
        print(f"   Week 7-8 ends: {self.week_dates['Week 7-8'].strftime('%Y-%m-%d')} (Jan 2, 2026)")

        print("\n⚠️  Manual Setup Still Needed:")
        print("   👤 Assignees: If user IDs not found, assign manually in ClickUp")


def main():
    parser = argparse.ArgumentParser(description="Import STATUS.md to ClickUp via API")
    parser.add_argument("--api-token", help="ClickUp API token (or set CLICKUP_API_TOKEN env var)")
    parser.add_argument("--list-id", help="ClickUp List ID (or set CLICKUP_LIST_ID env var)")
    parser.add_argument("--status-md", default="docs/STATUS.md", help="Path to STATUS.md file")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode (don't create tasks)")
    parser.add_argument(
        "--clean-orphaned",
        action="store_true",
        help="Delete tasks in ClickUp that are no longer in STATUS.md (use with caution)",
    )
    parser.add_argument(
        "--sprint-start",
        default="2025-11-09",
        help="Sprint start date (YYYY-MM-DD). Default: 2025-11-09",
    )
    parser.add_argument(
        "--service-name",
        default=None,
        help="Service name (auto-detected from script path if not provided)",
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
            else:
                print("❌ API token not found. Set --api-token or CLICKUP_API_TOKEN env var")
                print(f"   Checked: Secret Manager ({instruments_config.clickup_secret_name})")
                print("   Checked: Environment variables via settings.py")
                print("\n💡 To store API key in Secret Manager, run:")
                print("   python scripts/store_clickup_secret.py --api-key YOUR_TOKEN")
                return 1
        except Exception as e:
            print(f"⚠️  Secret Manager lookup failed: {e}")
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
        print("\n   Or right-click the list name → 'Copy link' and extract the ID")
        return 1

    status_md_path = Path(__file__).parent.parent / args.status_md
    if not status_md_path.exists():
        print(f"❌ STATUS.md not found at {status_md_path}")
        return 1

    print(f"✅ Using API token: {api_token[:20]}...")
    print(f"✅ Using List ID: {list_id}")
    print(f"✅ Reading STATUS.md from: {status_md_path}")
    print(f"✅ Sprint start date: {args.sprint_start}")
    if args.clean_orphaned:
        print("⚠️  CLEAN ORPHANED MODE: Will delete tasks not in STATUS.md")

    print()

    importer = ClickUpImporter(
        api_token,
        list_id,
        dry_run=args.dry_run,
        sprint_start_date=args.sprint_start,
        clean_orphaned=args.clean_orphaned,
        service_name=args.service_name,
    )
    importer.import_from_status_md(status_md_path)

    return 0


if __name__ == "__main__":
    exit(main())
