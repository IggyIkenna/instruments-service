# ClickUp Integration Guide

Complete guide for setting up and using the ClickUp API import script for importing STATUS.md data into ClickUp.

---

## Table of Contents

1. [Quick Start](#quick-start)
2. [Setup](#setup)
3. [What's Automated](#whats-automated)
4. [Manual Setup](#manual-setup)
5. [Finding Your List ID](#finding-your-list-id)
6. [Getting User IDs](#getting-user-ids)
7. [Usage](#usage)
8. [Troubleshooting](#troubleshooting)

---

## Quick Start

```bash
# 1. Get your API token from https://app.clickup.com/settings/apps
# 2. Find your List ID (see "Finding Your List ID" section below)
# 3. Run dry run first:
python scripts/clickup_import.py --dry-run

# 4. Run actual import:
python scripts/clickup_import.py
```

The script reads configuration from `instruments-service/.env.clickup` automatically.

---

## Setup

### Prerequisites

1. **ClickUp Account**: Free plan works fine (100 requests/minute limit)
2. **API Token**: Get from https://app.clickup.com/settings/apps
3. **List ID**: Found in ClickUp URL when viewing a list (see below)
4. **Python 3.9+**: Already installed for instruments-service

### Step 1: Get Your ClickUp API Token

1. Go to https://app.clickup.com/settings/apps
2. Click "Generate" under "API Token"
3. Copy the token (starts with `pk_...`)

### Step 2: Create Configuration File

Create `instruments-service/.env.clickup`:

```bash
# ClickUp Configuration for Instruments Service
# Each developer should update these with their own values

# ClickUp API Token (get from https://app.clickup.com/settings/apps)
clickup_api_token=pk_YOUR_TOKEN_HERE

# ClickUp List ID for Instruments Service
# Find in ClickUp URL: https://app.clickup.com/.../v/l/li/LIST_ID
clickup_list_id_instruments_service=YOUR_LIST_ID_HERE

# ClickUp User IDs (run 'python scripts/get_clickup_user_ids.py' to get these)
clickup_user_id_ikenna=YOUR_ID_HERE
clickup_user_id_harsh=YOUR_ID_HERE
clickup_user_id_femi=YOUR_ID_HERE
clickup_user_id_daniel=YOUR_ID_HERE
clickup_user_id_carlos=YOUR_ID_HERE
```

**Note**: The `.env.clickup` file is gitignored and won't be committed to version control.

### Step 3: Install Dependencies

The script only needs `requests`, which is already in `requirements.txt`:

```bash
pip install requests
```

---

## Finding Your List ID

### Quick Method

1. **Open your "Instruments Service" list** in ClickUp
2. **Look at the URL** in your browser's address bar
3. **Find the number after `/li/`** - that's your List ID!

### URL Format Examples

ClickUp URLs can have different formats:

**Format 1: Full URL**

```
https://app.clickup.com/12345678/v/l/li/98765432
```

**List ID**: `98765432` (the number after `/li/`)

**Format 2: Short URL**

```
https://app.clickup.com/98765432
```

**List ID**: `98765432` (the entire number)

**Format 3: With Folder**

```
https://app.clickup.com/12345678/v/f/12345678/98765432
```

**List ID**: `98765432` (the last number)

### Alternative Method

1. **Right-click** on "Instruments Service" in the sidebar
2. Click **"Copy link"** or **"Copy URL"**
3. **Paste** the URL somewhere
4. **Extract** the List ID from the URL (it's always a long number)

### Visual Guide

When you're viewing your list, the URL will look something like:

```
https://app.clickup.com/12345678/v/l/li/9876543212345678
                                    ^^^^^^^^^^^^^^^^^^^^
                                    This is your List ID!
```

### Still Can't Find It?

If you can't find the List ID in the URL:

1. Make sure you're **viewing the list** (not a task or folder)
2. The List ID is usually **8-15 digits long**
3. Try **right-clicking the list name** → "Copy link"
4. Check the **browser's developer console** (F12) → Network tab → look for API calls containing the List ID

The script will show you this guide if List ID is missing. Just run:

```bash
python scripts/clickup_import.py
```

---

## Getting User IDs

### Step 1: Run the Helper Script

Run this command to get your user IDs from ClickUp:

```bash
cd instruments-service
python scripts/get_clickup_user_ids.py
```

This will:

- Query ClickUp API for all team members
- Find users by username, email, or full name
- Display their user IDs
- Show you what to add to `.env.clickup`

### Step 2: Add User IDs to .env.clickup

After running the script, add these lines to your `.env.clickup` file:

```bash
clickup_user_id_ikenna=YOUR_ID_HERE
clickup_user_id_harsh=YOUR_ID_HERE
clickup_user_id_femi=YOUR_ID_HERE
clickup_user_id_daniel=YOUR_ID_HERE
clickup_user_id_carlos=YOUR_ID_HERE
```

Replace `YOUR_ID_HERE` with the actual user IDs from the script output.

### Step 3: Verify

Run the import script again:

```bash
python scripts/clickup_import.py --dry-run
```

You should see:

```
✅ Found Ikenna: YOUR_ID
✅ Found Harsh: YOUR_ID
```

### Alternative: Manual Method

If the script doesn't work, you can get user IDs manually:

1. **Via ClickUp API** (recommended):

   ```bash
   curl -H "Authorization: YOUR_API_TOKEN" https://api.clickup.com/api/v2/team
   ```

   Look for `"id"` in the `members` array for each user.

2. **Via ClickUp Web UI**:
   - Go to your ClickUp workspace
   - Click on your profile/avatar
   - The user ID is usually in the URL or can be found in browser dev tools
   - Or use the API endpoint above

### Troubleshooting User IDs

- **"Ikenna not found"**: Check if your ClickUp username contains "ikenna" or "igboaka" (case-insensitive)
- **"Harsh not found"**: Check if Harsh's ClickUp username contains "harsh" (case-insensitive)
- **API errors**: Make sure your API token is correct in `.env.clickup`

---

## What's Automated

### ✅ Fully Automated (No Manual Work Needed)

#### 1. **Task Creation** ✅

- ✅ All milestone tasks created
- ✅ All subtasks created and linked to parents
- ✅ In-progress tasks created
- ✅ Dependency tasks created (unified-cloud-services, Tardis API, Databento API)
- ✅ Task names automatically truncated if > 100 characters (full name preserved in description)
- ✅ Backticks in task names replaced with single quotes

#### 2. **Task Dependencies** ✅ AUTOMATIC

- ✅ "DeFi Instrument Support" → depends on → "unified-cloud-services Integration"
- ✅ "Databento Integration (TradFi)" → depends on → "Databento API Access"
- ✅ Linked automatically via ClickUp API after tasks are created
- **No manual work needed!**

#### 3. **Priorities** ✅ AUTOMATIC

- ✅ DeFi Instrument Support = **High** (priority 2)
- ✅ TradFi/Databento Integration = **Normal** (priority 3)
- ✅ Automatically set via API based on milestone name
- ✅ Visible in ClickUp as colored priority indicators
- **No manual work needed!**

#### 4. **Due Dates** ✅ AUTOMATIC

- ✅ Dates like "2025-01-15" automatically converted and set
- ✅ Week strings (e.g., "Week 5-6") converted to actual dates based on sprint start date
- ✅ Default sprint start: Nov 7, 2025 (can override with `--sprint-start`)
- ✅ Automatically set via API
- ✅ Visible in ClickUp list view and Gantt chart
- **Note**: Priorities and due dates are set for **milestones only**, not subtasks
- **No manual work needed!**

#### 5. **Tags** ✅ AUTOMATIC

- ✅ All tasks tagged with `instruments-service`
- ✅ Strategy tags: `defi`, `tradfi`, `options`
- ✅ Status tags: `milestone`, `in-progress`, `bug-fix`, `dependency`
- ✅ Week tags: `week-5-6`, `week-7-8`
- ✅ Automatically applied via API
- **No manual work needed!**

#### 6. **Custom Fields** ✅ AUTOMATIC

- ✅ Custom fields created automatically:
  - Coverage %
  - Test Coverage %
  - DRY Compliance %
  - Week (dropdown)
  - Strategy (multi-select)
- ✅ Week values populated automatically ("Week 5-6", "Week 7-8")
- ✅ Automatically created and populated via API
- **No manual work needed!**

#### 7. **Task Hierarchy** ✅ AUTOMATIC

- ✅ Subtasks properly nested under parent tasks
- ✅ Parent-child relationships maintained
- ✅ Automatically set via API (`parent` field)
- **No manual work needed!**

#### 8. **Task Status** ✅ AUTOMATIC

- ✅ Tasks marked as "complete", "to do", or "in progress" based on STATUS.md
- ✅ Automatically set via API
- **No manual work needed!**

#### 9. **Task Descriptions** ✅ AUTOMATIC

- ✅ Notes from STATUS.md added as task descriptions
- ✅ Full task names added to description if truncated
- ✅ Automatically set via API
- **No manual work needed!**

#### 10. **Assignees** ✅ AUTOMATIC (if user IDs configured)

- ✅ Tasks automatically assigned based on content:
  - Infrastructure/backfill tasks → Femi
  - DeFi/TradFi tasks → Harsh
  - General tasks → Ikenna
- ✅ Requires user IDs in `.env.clickup`
- **No manual work needed if configured!**

#### 11. **Idempotency** ✅ AUTOMATIC

- ✅ Rerunning script updates existing tasks instead of creating duplicates
- ✅ Checks for existing tasks by name
- ✅ Updates existing custom fields instead of recreating
- **Safe to run multiple times!**

#### 12. **Orphaned Task Cleanup** ✅ OPTIONAL

- ✅ Use `--clean-orphaned` flag to delete tasks not in STATUS.md
- ✅ Only deletes tasks with `instruments-service` tag (safe)
- ✅ Warns about orphans by default (doesn't delete)

---

## Manual Setup

### ⚠️ Manual Setup Required (~5 minutes)

#### 1. **Sprint Start Date** ⚠️ Optional

**Why**: Week dates (e.g., "Week 5-6") are calculated based on sprint start date

**What to do**:

- Default sprint start: Nov 7, 2025
- Override with `--sprint-start YYYY-MM-DD` flag
- Example: `--sprint-start 2025-01-15`

**Week Date Calculation**:

- Sprint start: Nov 7, 2025
- Week 1 ends: Nov 14, 2025
- Week 5-6 ends: Dec 19, 2025
- Week 7-8 ends: Jan 2, 2026

#### 2. **Assignees** ⚠️ Manual (if not configured)

**Why**: If user IDs not in `.env.clickup`, tasks won't be assigned automatically

**What to do**:

1. Run `python scripts/get_clickup_user_ids.py` to get user IDs
2. Add to `.env.clickup` (see "Getting User IDs" section)
3. Or assign manually in ClickUp:
   - Use ClickUp AI: "Assign all DeFi tasks to [team member name]"
   - Or assign manually per task

---

## Usage

### Dry Run First (Recommended)

Test the script without creating tasks:

```bash
cd instruments-service
python scripts/clickup_import.py --dry-run
```

### Actual Import

Once dry run looks good:

```bash
python scripts/clickup_import.py
```

### Clean Orphaned Tasks

Delete tasks in ClickUp that are no longer in STATUS.md:

```bash
python scripts/clickup_import.py --clean-orphaned
```

**Warning**: Only deletes tasks with `instruments-service` tag. Use with caution!

### Command Line Options

```bash
python scripts/clickup_import.py [OPTIONS]

Options:
  --api-token TOKEN      ClickUp API token (or set in .env.clickup)
  --list-id ID           ClickUp List ID (or set in .env.clickup)
  --status-md PATH       Path to STATUS.md (default: docs/STATUS.md)
  --dry-run              Dry run mode (don't create tasks)
  --clean-orphaned       Delete tasks not in STATUS.md (use with caution)
  --sprint-start DATE    Sprint start date YYYY-MM-DD (default: 2025-11-07)
```

### Environment Variables

The script reads from `instruments-service/.env.clickup` (or root `.env` for backwards compatibility):

```bash
clickup_api_token=pk_YOUR_TOKEN
clickup_list_id_instruments_service=YOUR_LIST_ID
clickup_user_id_ikenna=YOUR_ID
clickup_user_id_harsh=YOUR_ID
# ... etc
```

---

## Troubleshooting

### "List not found" Error

- Verify List ID is correct
- Check that you have access to the list
- List ID is in URL: `https://app.clickup.com/.../v/l/li/LIST_ID`
- Remove `li/` prefix if present (script handles this automatically)

### Rate Limit Errors

- Script handles this automatically
- Free plan: 100 requests/minute
- May take 2-3 minutes for full import
- Script waits automatically when approaching limits

### Custom Fields Not Created

- Some custom field types may require paid plan
- Script will continue even if custom fields fail
- You can create them manually in ClickUp
- Check error message for specific issue

### Subtasks Not Linked

- Parent task must be created first
- Script creates parents before subtasks
- Check task names match exactly
- Verify parent ID is correct

### Task Name Too Long

- ClickUp limits task names to 100 characters
- Script automatically truncates to 100 chars
- Full name preserved in task description
- Check description for complete name

### 400 Bad Request Errors for Subtasks

- **Common cause**: Parent task ID format issue
- **Solution**: The script now includes better error logging - check the API error details
- **Debug**: Run with `--dry-run` first to see what would be created
- **Check**: Verify parent task exists in ClickUp before creating subtasks
- **Note**: Parent field must be a valid task ID (not list ID)

### User IDs Not Found

- Run `python scripts/get_clickup_user_ids.py` to find user IDs
- Check `.env.clickup` file exists and has correct format
- Verify API token has access to team members
- Check username/email spelling in script output

### Orphaned Tasks Warning

- Normal if tasks were renamed in STATUS.md
- Use `--clean-orphaned` to delete (careful!)
- Or delete manually in ClickUp
- Only affects tasks with `instruments-service` tag

### Data Catalogue Subtasks Not Found

- **Problem**: Data catalogue subtasks can't find parent milestones
- **Cause**: Milestone names may have markdown formatting (`**text**`) in STATUS.md
- **Solution**: Script automatically normalizes milestone names (strips markdown formatting)
- **Debug**: Check output for "Available milestones in task_map" and "Available milestones in existing_tasks"
- **Fix**: Ensure milestone names in STATUS.md Timeline Tracking table match exactly (markdown is auto-stripped)

### Parent Milestone Lookup Issues

- **Problem**: "Could not find parent milestone for strategy 'X'"
- **Cause**: Milestone may not exist yet, or name mismatch due to formatting
- **Solution**:
  1. Run `instruments-service` import first (creates parent milestones)
  2. Script uses normalized matching (handles markdown formatting automatically)
  3. Check debug output to see what milestones are available
  4. Ensure strategy names in Data Catalogue table match milestone names (e.g., "ML Delta-One" not "Delta-One ML")

---

## Features

### Rate Limiting

- Automatically handles ClickUp's rate limits (100 requests/minute on free plan)
- Waits when approaching limits
- Retries on 429 errors
- Progress indicators during wait

### Error Handling

- Graceful error handling
- Continues on individual task failures
- Reports errors clearly with helpful messages
- Shows which tasks succeeded/failed

### Dry Run Mode

- Test before importing
- See what would be created
- No actual API calls
- Shows all details (tags, priorities, dates, etc.)

### Idempotency

- Safe to run multiple times
- Updates existing tasks instead of creating duplicates
- Checks for existing custom fields
- Preserves manual changes in ClickUp

---

## What Gets Imported

### Automatically Uploaded:

1. ✅ **All tasks** (~60+ tasks total)
2. ✅ **All subtasks** (properly nested)
3. ✅ **Dependencies** (automatically linked)
4. ✅ **Priorities** (DeFi=High, TradFi=Normal)
5. ✅ **Due dates** (absolute dates and week dates)
6. ✅ **Tags** (all tags applied)
7. ✅ **Custom fields** (created and populated)
8. ✅ **Task descriptions** (notes from STATUS.md)
9. ✅ **Task status** (complete/to do/in progress)
10. ✅ **Assignees** (if user IDs configured)

### Not Uploaded (Manual):

1. ⚠️ **Sprint start date** (can override with `--sprint-start`)
2. ⚠️ **Assignees** (if user IDs not in `.env.clickup`)

## Viewing Milestones in ClickUp

### In List View

1. Open your "Instruments Service" list
2. Milestones are tasks tagged with `milestone`
3. Filter by tag: Click "Filter" → Select "Tags" → Choose `milestone`

### In Gantt View (Recommended)

1. Click the `+ View` button in your Views Bar
2. Select `Gantt`
3. Milestones appear as diamond icons on the timeline
4. You can see dependencies, due dates, and timeline relationships

### Filter by Milestones

1. Click `Filter` in the upper-right corner
2. Select `Task type`
3. Choose `Milestone` (if using ClickUp's milestone feature)
4. Or filter by tag: `milestone`

### Viewing Priorities and Due Dates

- **Priorities**: Shown as colored dots/bars next to task names
  - Red = Urgent (priority 1)
  - Orange = High (priority 2)
  - Yellow = Normal (priority 3)
  - Gray = Low (priority 4)
- **Due Dates**: Shown in task list and Gantt view
- **Custom Fields**: Click on a task to see Week and Strategy custom fields

---

**✅ Automated (95% of work)**:

- All tasks, subtasks, dependencies, priorities, dates, tags, custom fields
- Everything from STATUS.md is imported automatically
- Takes ~2-3 minutes on free plan (rate limiting handled)
- Safe to rerun (idempotent)

**⚠️ Manual (5% of work, ~5 minutes)**:

- Configure user IDs in `.env.clickup` (optional)
- Override sprint start date if needed (optional)

**Result**: Fully functional ClickUp project with 95% automation! 🎉

The script is production-ready and handles everything automatically including:

- Task name truncation
- Character cleaning (backticks)
- Subtask parent linking
- Dependency linking
- Rate limiting
- Error recovery
- Idempotency
