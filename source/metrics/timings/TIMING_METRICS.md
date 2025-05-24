# Timing Metrics Fact Sheet

This document provides an overview of the timing-based metrics, their purpose, key calculations, and important parameters or heuristics used.

## 1. Diff Delta Metric (`DiffDeltaMetric`)

*   **Purpose:** Implements GitClear's "Diff Delta" to measure meaningful code contributions by quantifying the cognitive load of code changes. It aims to provide a more nuanced view of effort than raw line counts.
*   **Key Calculations & Logic:**
    *   **Diff Delta Score:** For each file modification, Diff Delta is calculated as:
        `Σ (weight_operation * number_of_meaningful_lines_for_operation)`
        Operations include 'add', 'update', 'delete', 'move'.
    *   **Weights:**
        *   `add`: 1.0 (New code has highest cognitive load)
        *   `update`: 0.75 (Modifying existing code)
        *   `delete`: 0.25 (Removing code is easier)
        *   `move`: 0.1 (Moving/renaming has lowest load)
    *   **Meaningful Lines (`_is_meaningful_line`):**
        *   Excludes lines that are empty, whitespace-only, or very short (e.g., single characters like '{').
        *   Excludes common comment patterns for various languages (Python, Java, C++, JS, HTML, SQL, etc.).
        *   Excludes common import/include/package statements for various languages.
        *   *TODO for `_is_meaningful_line`*: Consider refining handling of very short lines and import statements (e.g., low weight instead of skip).
    *   **Skipped Files (`_should_skip_file`):**
        *   Excludes lock files (e.g., `package-lock.json`, `yarn.lock`, `Gemfile.lock`).
        *   Excludes minified files (`*.min.*`).
        *   Excludes source maps (`*.map`).
        *   Excludes files matching "generated" or "auto" patterns.
        *   Excludes common build artifact folders (e.g., `dist/`, `build/`, `target/`).
        *   Excludes vendor/dependency folders (e.g., `vendor/`, `node_modules/`).
        *   Excludes IDE/editor specific files/folders (e.g., `.vscode/`, `.idea/`, `.DS_Store`).
        *   Excludes version control folders (`.git/`).
        *   Excludes common binary/data file extensions (e.g., images, fonts, archives).
        *   *TODO for `_should_skip_file`*: List of skippable files should be configurable or more context-aware, especially for data/config files like JSON, XML, YAML.
    *   **Moved Lines (`_detect_moved_lines`):**
        *   Identified by comparing stripped content of added and deleted lines within the same commit.
        *   These lines contribute to Diff Delta with the 'move' weight.
        *   *Comment*: May not perfectly distinguish pure content moves from lines deleted and re-added with only whitespace/indentation changes.
    *   **Updated Lines:**
        *   Calculated as `updates = min(meaningful_added_lines - moved_lines, meaningful_deleted_lines - moved_lines)`.
        *   A heuristic is applied: `meaningful_updates = int(updates * 0.8)`.
        *   *Heuristic Rationale*: Assumes 80% of lines involved in an update are meaningful, acknowledging some churn or minor refactoring.
*   **Output Metrics (per developer, weekly and total):**
    *   `diff_delta`: Total Diff Delta score.
    *   `lines_added`, `lines_updated`, `lines_deleted`, `lines_moved`: Counts of meaningful lines for each category.
    *   `commits`: Number of commits contributing to the Diff Delta.
    *   `files_changed`: Number of unique files changed.
    *   `active_days`: Number of unique days with commits.
    *   `velocity_per_day`: `diff_delta / active_days`.
*   **Week Key (`_get_week_key`):** `YYYY-MM-DD` (Monday of the week).

## 2. Developer Hours Metric (`DeveloperHoursMetric`)

*   **Purpose:** Estimates developer coding hours based on commit timestamps and patterns, inspired by GitClear's methodology. It aims to provide conservative estimates of active coding time.
*   **Key Calculations & Logic:**
    *   **Session Identification:**
        *   Commits are grouped into sessions. A new session starts if the time since the last commit in the current session exceeds `max_commit_gap`.
        *   `default_first_commit_time` is subtracted from the first commit's timestamp to estimate session start.
        *   `default_last_commit_time` is added to the last commit's timestamp in a session to estimate session end.
        *   Sessions shorter than `min_session_length` are discarded.
    *   **Session Parameters (based on GitClear research):**
        *   `max_commit_gap`: 2 hours (Max time between commits in the same session).
        *   `min_session_length`: 30 minutes (Minimum valid session duration).
        *   `default_first_commit_time`: 30 minutes (Estimated work time before first commit).
        *   `default_last_commit_time`: 15 minutes (Estimated wrap-up time after last commit).
    *   **Session Hour Calculation (`_calculate_session_hours`):**
        *   Initial hours = `session_duration.total_seconds() / 3600`.
        *   **Adjustments (Heuristics):**
            *   If `commit_count == 1`: `hours *= 0.75` (Single commit sessions might be overestimated).
            *   If `commit_count > 10`: `hours *= 0.9` (Many commits might indicate automated activity).
            *   `changes_per_hour = total_raw_lines_changed / max(0.1, hours)`:
                *   If `changes_per_hour > 1000`: `hours *= 0.8` (Very high change rate might indicate generated code).
                *   If `changes_per_hour < 50`: `hours *= 1.1` (Low change rate might indicate research/debugging).
            *   *Note on `changes`*: Currently uses raw line counts (`insertions + deletions`). Future enhancement could use 'meaningful' changes.
        *   Session length is capped at 8 hours.
*   **Output Metrics (per developer, weekly and total):**
    *   `total_estimated_hours`: Sum of estimated hours for all sessions.
    *   `total_sessions`: Total number of valid coding sessions.
    *   `weekly_hours`: Dictionary per week:
        *   `estimated_hours`: Hours for that week.
        *   `sessions`: Session count for that week.
        *   `commits`: Commit count for that week.
        *   `hours_per_day`: `estimated_hours / number_of_productive_days_in_week`.
*   **Week Key (`_get_week_key`):** `YYYY-Www` (e.g., `2023-W34`, ISO week format).

## 3. Comprehensive Time Analysis Metric (`ComprehensiveTimeAnalysisMetric`)

*   **Purpose:** Provides a broad analysis of developer activity based on commit timestamps, focusing on work patterns, rhythms, and consistency.
*   **Key Calculations & Logic:**
    *   Stores author activities (timestamp, repo, commit_hash, lines_changed, files_changed).
    *   `lines_changed` currently uses raw sum of `mf.added_lines + mf.deleted_lines`. (Future enhancement: use 'meaningful' changes).
    *   **Inter-Commit Intervals:** Time difference (in seconds) between consecutive commits by an author.
    *   **Work Session Identification (`_identify_work_sessions`):**
        *   `session_gap_hours`: 4 hours. Commits within this gap are part of the same session.
        *   Calculates `session_count`, average/max session length (hours), average/max commits per session.
    *   **Daily Patterns (`_analyze_daily_patterns`):**
        *   Commit counts per day of the week (0=Monday, 6=Sunday).
        *   Identifies `peak_day` and `peak_day_count`.
        *   Provides `day_distribution` (weekday -> count).
    *   **Weekly Patterns (`_analyze_weekly_patterns`):**
        *   Commit counts per week (`YYYY-Www`, using `%U` for week number).
        *   Calculates `total_weeks`, average/max/min activities per week.
    *   **Downtime Analysis (`_analyze_downtime_patterns`):**
        *   Based on inter-commit intervals (converted to hours).
        *   `short_breaks_count`: Intervals <= 4 hours.
        *   `long_breaks_count`: Intervals > 24 hours.
        *   `avg_break_hours`.
    *   **Coding Rhythm (`_analyze_coding_rhythm`):**
        *   `activities_per_day`: `total_commits / total_span_days`.
        *   `consistency_score`: `work_sessions_count / total_span_days`.
    *   **Sustained Activity (`_analyze_sustained_activity`):**
        *   `total_active_days`: Count of unique days with commits.
        *   `max_consecutive_days`: Longest streak of consecutive days with commits.
        *   `avg_activities_per_active_day`.
*   **Output Metrics (per developer):**
    *   `basic_stats`: `total_commits`, `total_repos` (see merge note), `first_commit_date`, `last_commit_date`, `total_span_days`, `commits_per_day`, `total_lines_changed`, `total_files_changed`.
    *   `timing_patterns`: `mean_interval_hours`, `median_interval_hours`, `min_interval_minutes`, `max_interval_days`.
    *   `work_sessions`: As described above.
    *   `daily_patterns`: As described above.
    *   `weekly_patterns`: As described above.
    *   `downtime_analysis`: As described above.
    *   `rhythm_analysis`: As described above.
    *   `sustained_activity`: As described above.
*   **Merge Logic Notes:**
    *   The `merge_metrics` static method has been improved but has limitations for sub-metrics that ideally require original activity data for perfect merging (e.g., `timing_patterns`, some averages in `work_sessions`, `weekly_patterns`, `downtime_analysis`, `sustained_activity`). These are noted with TODOs in the code and currently use strategies like overwriting with the last instance's data or simpler aggregations. `basic_stats`, `daily_patterns` (distribution), and `rhythm_analysis` are merged more accurately.

## 4. Proposed New Timing Metrics

This section outlines potential new timing-related metrics that could provide further insights into developer activity and productivity. Implementation of these metrics may require additional data sources (e.g., issue tracking, PR platforms) or more complex analysis of commit history.

### a. Time to First Commit (TTFC)

*   **Purpose:** Measures the time elapsed from when a developer starts working on a task/issue to their first commit related to that task.
*   **Calculation Idea:** `Timestamp of first commit (for task) - Timestamp of task assignment/start_work_event`.
*   **Data Required:** Integration with issue tracking systems (e.g., Jira, GitHub Issues) to get task assignment times or "start work" events. Commit messages would need to be parsable to link commits to tasks (e.g., `PROJ-123: Fix login bug`).
*   **Insights:** Can indicate how quickly developers begin implementation, potential delays in starting tasks, or the complexity of initial setup for a task.

### b. Review Time / Time to Merge (TTM)

*   **Purpose:** Measures the duration of the code review process, from when a change is ready for review (e.g., Pull Request creation) until it's merged.
*   **Calculation Idea:**
    *   `Review Time = Timestamp of PR merge - Timestamp of PR creation`.
    *   Could be broken down further: `Time to First Review`, `Time from Last Approval to Merge`.
*   **Data Required:** Integration with repository platforms that support Pull Requests (e.g., GitHub, GitLab, Bitbucket). Access to PR event data (creation, reviews, approvals, merges).
*   **Insights:** Highlights bottlenecks in the review process, review efficiency, and overall cycle time for changes.

### c. Focus Factor

*   **Purpose:** Estimates the proportion of a developer's active day that is spent in focused coding sessions.
*   **Calculation Idea:** `Total duration of coding sessions (from DeveloperHoursMetric) in a day / (Timestamp of last commit of the day - Timestamp of first commit of the day)`.
*   **Data Required:** Uses outputs from `DeveloperHoursMetric` (session durations) and commit timestamps.
*   **Insights:** Could indicate days with highly focused work versus days with more fragmented activity or other non-coding tasks. A low focus factor might suggest many interruptions or context switching.

### d. Context Switch Frequency

*   **Purpose:** Measures how often a developer switches between different projects or significant work contexts within a defined period (e.g., a day or a work session).
*   **Calculation Idea:** Count the number of times a developer makes a commit to a different repository (or a significantly different module within a monorepo, if identifiable) after having worked on another.
*   **Data Required:** Commit history, including repository information for each commit. `ComprehensiveTimeAnalysisMetric`'s `author_activities` (which includes repo path) could be a basis.
*   **Insights:** Frequent context switching can be detrimental to productivity. This metric could help identify patterns or individuals who might be spread too thin.

### e. Session Churn / Revision Rate

*   **Purpose:** Measures how much code written by an author within a coding session (or shortly thereafter) is then quickly revised or deleted by the same author. This is distinct from overall code churn measured over longer periods.
*   **Calculation Idea:**
    *   Identify lines added by an author within a defined session.
    *   Track how many of those specific lines are deleted or heavily modified by the *same author* within that session or a short subsequent period (e.g., next 1-2 sessions or within 24-48 hours).
    *   `Session Churn Rate = (Lines churned by self shortly after creation) / (Lines created in session)`.
*   **Data Required:** Detailed line-level commit history and authorship. Requires careful tracking of line provenance. This is a more complex metric to implement accurately.
*   **Insights:** Could indicate initial implementation quality, amount of self-correction/refinement, or a "trial and error" coding style. High session churn might not always be negative (e.g., iterative refinement) but could also point to inefficiencies.

## 5. Suggested Unit Tests for Timing Metrics

This section outlines key areas and test case scenarios for unit testing the timing metrics. Comprehensive unit testing is crucial for ensuring accuracy and reliability of these metrics.

### General Test Structure Considerations:

*   **Mock Commit Data:** Create mock `Commit` objects (and `ModifiedFile` objects) with varying attributes (timestamps, author, lines added/deleted, file types, diffs) to simulate different scenarios.
*   **Metric Instances:** Instantiate the metric classes and process the mock commits.
*   **Assert Outputs:** Compare the results from `get_metrics()` and `merge_metrics()` against expected values.

### A. `DiffDeltaMetric` Tests

*   **`_is_meaningful_line(line)`:**
    *   Test with various empty/whitespace lines (should return `False`).
    *   Test with single-character lines (should return `False`).
    *   Test with various comment styles for different languages (Python, Java, C++, HTML, SQL, etc.) (should return `False`).
    *   Test with various import/include statements (should return `False`).
    *   Test with actual code lines of different lengths (should return `True`).
    *   Test with lines containing mixed code and comments (behavior depends on regex; typically `True` if code part exists and isn't a comment).
*   **`_should_skip_file(filename)`:**
    *   Test with various skippable file names/paths (minified, lock files, generated, build artifacts, vendor, IDE specific, binaries) (should return `True`).
    *   Test with typical source code file names (e.g., `*.py`, `*.java`, `*.js`) (should return `False`).
    *   Test edge cases like files with multiple extensions or in nested skippable directories.
*   **`_detect_moved_lines(added_lines, deleted_lines)`:**
    *   Test with no moved lines (added and deleted are distinct).
    *   Test with all lines moved (added and deleted are identical after stripping).
    *   Test with partially moved lines.
    *   Test with lines that differ only by whitespace/indentation (current behavior: may not detect as move if stripped content is same).
*   **`process_modified_file(...)` (core Diff Delta calculation):**
    *   Test a commit with only additions of meaningful lines.
    *   Test a commit with only deletions of meaningful lines.
    *   Test a commit with only updates (ensure 80% heuristic is applied).
    *   Test a commit with only moved lines.
    *   Test a commit with a mix of adds, deletes, updates, and moves.
    *   Test with a commit modifying a skipped file (should result in 0 Diff Delta for that file).
    *   Test with a commit where all lines are non-meaningful (should result in 0 Diff Delta).
*   **`get_metrics()`:**
    *   Test with a sequence of commits from a single author, spanning multiple weeks.
    *   Verify `total_diff_delta`, `total_commits`, and weekly aggregations (`diff_delta`, `lines_added`, etc., `velocity_per_day`).
*   **`merge_metrics(metrics_list)`:**
    *   Test merging data from two metric instances for the same author, same weeks.
    *   Test merging data for different authors.
    *   Test merging data where weeks overlap and where they are distinct.
    *   Ensure `velocity_per_day` is correctly recalculated.

### B. `DeveloperHoursMetric` Tests

*   **Session Calculation (`calculate_sessions` and `_process_sessions_to_weekly_stats`):**
    *   Test with a single commit (should form a session with default start/end times).
    *   Test with multiple commits within `max_commit_gap` (should form one session).
    *   Test with commits exceeding `max_commit_gap` (should form multiple sessions).
    *   Test sessions that are shorter than `min_session_length` (should be discarded).
    *   Test session boundaries (e.g., commit exactly at `max_commit_gap`).
*   **`_calculate_session_hours(session)`:**
    *   Test a session with 1 commit (verify `* 0.75` adjustment).
    *   Test a session with >10 commits (verify `* 0.9` adjustment).
    *   Test a session with high `changes_per_hour` (>1000) (verify `* 0.8` adjustment).
    *   Test a session with low `changes_per_hour` (<50) (verify `* 1.1` adjustment).
    *   Test a session that would exceed 8 hours before capping (verify it's capped at 8.0).
    *   Test with `changes = 0` (ensure no division by zero if hours is also small).
*   **`_get_week_key(date)`:**
    *   Test various dates to ensure correct `YYYY-Www` format.
    *   Test dates at year/week boundaries.
*   **`get_metrics()`:**
    *   Test with a sequence of commits forming various sessions over weeks.
    *   Verify `total_estimated_hours`, `total_sessions`, and weekly `estimated_hours`, `sessions`, `commits`, `hours_per_day`.
*   **`merge_metrics(metrics_list)`:**
    *   Test merging for the same author, same weeks (ensure `productive_days` sets are combined, hours summed, `hours_per_day` recalculated).
    *   Test merging with different authors or different weeks.

### C. `ComprehensiveTimeAnalysisMetric` Tests

*   **`_identify_work_sessions(activities, session_gap_hours)`:**
    *   Test with no activities, <2 activities.
    *   Test commits forming a single session.
    *   Test commits forming multiple sessions based on `session_gap_hours`.
    *   Verify calculations for `session_count`, `avg_session_length_hours`, `max_session_length_hours`, `avg_commits_per_session`, `max_commits_per_session`.
*   **`_analyze_daily_patterns(activities)`:**
    *   Test with activities to ensure correct `peak_day`, `peak_day_count`, and `day_distribution`.
*   **`_analyze_weekly_patterns(activities)`:**
    *   Test with activities spanning multiple weeks to verify `total_weeks`, `avg_activities_per_week`, etc.
*   **`_analyze_downtime_patterns(intervals)`:**
    *   Test with various inter-commit intervals to verify `short_breaks_count`, `long_breaks_count`, `avg_break_hours`.
*   **`_analyze_coding_rhythm(activities, work_sessions)`:**
    *   Verify `activities_per_day` and `consistency_score`.
*   **`_analyze_sustained_activity(activities)`:**
    *   Test with various activity patterns to verify `total_active_days`, `max_consecutive_days`, `avg_activities_per_active_day`.
*   **`get_metrics()`:**
    *   Test with a comprehensive set of activities for an author.
*   **`merge_metrics(metrics_list)`:**
    *   **`basic_stats`**: Test summation of counts, min/max of dates, recalculation of `total_span_days`, `commits_per_day`. Test `total_repos` overwrite.
    *   **`timing_patterns`**: Verify overwrite behavior and acknowledge with TODOs.
    *   **`work_sessions`**: Test summation of `session_count`, max for max values, overwrite for averages/list, and acknowledge with TODOs.
    *   **`daily_patterns`**: Test merging of `day_distribution` and recalculation of peak stats.
    *   **`weekly_patterns`**: Test summation/overwrite and acknowledge with TODOs.
    *   **`downtime_analysis`**: Test summation of counts, overwrite of average, and acknowledge with TODOs.
    *   **`rhythm_analysis`**: Test recalculation based on merged stats.
    *   **`sustained_activity`**: Test summation/overwrite and acknowledge with TODOs.
