Here we go — **Phase 2: state/action encoding design** in markdown form that you can drop in as e.g.
`docs/telemetry_state_action_design_v0.2.md`.

````markdown
# Minecraft Auto Miner – Telemetry State/Action Encoding Design v0.2  
(Phase 2 – Discrete State, Action, Reward Design)

**Date:** 2025-12-08  
**Target version:** app v0.5.x + telemetry v0.2  
**Scope:** Define how we convert high-volume telemetry (logs, F3, metrics) into compact **state → action → reward** tuples that the miner can learn from _without_ human intervention during normal runs.

---

## 0. Goals

We want a miner that can **self-tune** and eventually **self-learn** by consuming telemetry:

- Turn raw logs (miner logs, F3 console data, Minecraft logs) into:
  - **Discrete states**: “what situation am I in?”
  - **Actions**: “what knobs can I turn?” (profile, control params, lane mode, etc.)
  - **Rewards**: “how good was that choice?” (blocks/sec minus penalties).
- Store this in Postgres in a **compressed, queryable format** suitable for:
  - Offline analysis (Jupyter, notebooks, dashboards).
  - Online policy updates (strategy + control tuning refinement).
- Keep overhead under ~**1KB/sec** and avoid touching the main gameplay loop.

This document defines **the encoding scheme**; wiring/implementation comes in Phase 3.

---

## 1. Telemetry Sources and Levels

### 1.1 Current sources (already available)

1. **Miner log (`logs/mining_helper.log`)**
   - Episode start/end messages.
   - Profile name, window length.
   - `MetricsManager` summaries per run.
   - StuckDetector / ResetDetector events.
   - Watchdog events (low-progress, wallhug).
   - Strategy decisions (next profile).
   - Control tuning decisions (chosen params, reward logs if we add them).

2. **Control + Strategy configs**
   - `control_tuning.ControlParams` (pitch, sprint_seconds, strafe_pattern).
   - `MiningProfile` metadata (window seconds, baseline strategy).
   - World-model metadata (lane spacing, lane execution flag).

3. **Perception frame flags (via log summaries)**
   - `has_block_break`, mining ratio, etc. (aggregated by `MetricsManager`).

### 1.2 Near-future sources (Phase 2.5 / 3)

1. **F3 Console snapshot stream**
   - Position (X, Y, Z), facing.
   - Chunk info, biome, light levels (if needed later).
   - Block looking at (block type, coordinates).
   - Used mainly to:
     - Validate pose provider.
     - Enrich state with environment type.

2. **Minecraft log (server/client log)**
   - Errors, teleport confirmations, lag spikes, anti-cheat/stuck messages.
   - Used primarily as **auxiliary labels/penalty signals**.

---

## 2. Core Telemetry Entities

We standardize on four main logical entities:

1. **Episode**  
   A single mining “run” between `metrics.start_run()` and `metrics.end_run()`.

2. **Tick**  
   One main loop iteration (`tick_interval_seconds` ~ 0.08 s).

3. **Window**  
   A **sliding summary** over a contiguous span of ticks (≈ metrics window or watchdog window).

4. **State–Action–Reward (SAR) tuple**  
   A compact data point:

   ```text
   (episode_id, t_window_start, state_vector, action_vector, reward_scalar)
````

---

## 3. State Encoding Design

We define state at **three levels**: episode, window, and local context. Phase 2 will focus on **window-level** state for learning.

### 3.1 State granularity & IDs

* **Episode ID**

  * UUID or monotonically increasing integer per run.
  * Created at `start_episode()` and logged once.
* **Window ID / timestamp**

  * Derived from:

    * `episode_id`
    * `window_index` or `window_start_time_utc`

For learning, we treat each **window** as one data point:
“Given this state at window W, and the chosen action A for this episode, we observed reward R at the end.”

### 3.2 Window-level state vector (v0.2)

For each window (aligned with `MetricsManager` windows), we encode:

#### 3.2.1 Performance features

* `blocks_per_minute`
* `mining_ratio` (mining_ticks / ticks)
* `stuck_events_in_window` (0/1, count)
* `reset_events_in_window` (0/1, count)
* `watchdog_low_progress_triggered` (0/1)
* `watchdog_wallhug_triggered` (0/1)

Derived from metrics + logs.

#### 3.2.2 Environment & pose features (from world_model/F3)

* Pose-based (from `pose_provider` or F3):

  * `facing_dir` (N/S/E/W → categorical or one-hot)
  * `y_level_bucket` (surface, mid, deep, etc.)
  * `x_in_lane_span` (position relative to lane bounds, normalized)

* Lane/world model:

  * `lane_index` (integer, or “hub” if not yet in lane)
  * `lane_phase` (e.g. IDLE, WALKING_TO_LANE, MINING_LANE, RETURNING)
  * `lane_execute_flag` (0/1)

#### 3.2.3 Profile & control features

* `profile_name` (hashed or indexed)
* `profile_window_seconds`
* Control tuning params for the episode:

  * `pitch_pixels`
  * `sprint_seconds`
  * `strafe_pattern_id` (lookup from a small enum)

These are **constant per episode** but repeated in each window row for simplicity in downstream analysis.

#### 3.2.4 Configuration context (coarse, for later clustering)

We optionally record a few coarse “config knobs” that matter:

* `tick_interval_seconds`
* `watchdog_window_seconds`
* `low_progress_threshold_bpm`
* `wallhug_window_seconds`
* `wallhug_threshold_bpm`

These help separate data generated from different settings.

---

## 4. Action Encoding Design

The “action” is **what choice the agent is making** at the *start of the episode* (or, later, mid-episode). For Phase 2, we keep it **episode-level** and aligned with control_tuning + strategy.

### 4.1 Action types (v0.2)

We define a small, discrete action space:

1. **Profile Choice**

   * `profile_id` ∈ {0..N_profiles-1}
   * Current logic: `StrategyManager.select_next_profile()` chooses this.

2. **Control Param Choice**

   * Triplet `(pitch_bucket, sprint_bucket, strafe_pattern_id)`
   * Relates directly to `ControlParams`:

     * `pitch_pixels` ∈ [min_pitch, max_pitch] → discretized into bins
     * `sprint_seconds` ∈ [min_sprint, max_sprint] → discretized into bins
     * `strafe_pattern` ∈ small finite set, mapped to integer ID

3. **Lane mode choice (for later)**

   * `lane_execute_flag` ∈ {0, 1}
   * For v0.2, we treat this as a fixed config, but it’s part of the future action space.

### 4.2 Action vector representation

For each episode:

```text
action = {
  profile_id: int,
  pitch_bucket: int,
  sprint_bucket: int,
  strafe_pattern_id: int,
  lane_execute_flag: int,  # 0/1; constant per run in v0.2
}
```

Implementation detail:

* At `start_episode()` we already know:

  * `active_profile.name`
  * `current_episode_params.pitch_pixels`
  * `current_episode_params.sprint_seconds`
  * `current_episode_params.strafe_pattern`
  * `lanes_execute`
* We encode these into discrete buckets and log a **single “ACTION_CHOSEN” event** with both the raw values and the bucketed indices.

---

## 5. Reward Design

The reward should reflect **mining efficiency** minus **penalties** for being stuck or grinding walls.

### 5.1 Episode reward

At `end_episode()` we already compute:

* `episode_duration_seconds`
* `blocks_per_minute` (from aggregate `WindowStats`)
* `stuck` / `reset` reasons via flags.

We define the **raw reward**:

```text
reward_raw = blocks_per_minute
```

### 5.2 Penalties

We then subtract penalties:

* `penalty_stuck`:

  * If episode ended because of a `stuck_reason`, apply a fixed penalty.
* `penalty_reset`:

  * If episode ended due to `reset_detected`.
* `penalty_wallhug`:

  * If high-BPM wallhug watchdog triggered at least once in the episode.
* `penalty_short_episode`:

  * If `duration < min_episode_duration` or `block_breaks < min_valid_blocks`, we either:

    * Mark episode as `invalid`, **or**
    * Assign a very low reward (depending on mode).

Proposed formula:

```text
reward = blocks_per_minute
         - 20 * (stuck_episode ? 1 : 0)
         - 10 * (reset_episode ? 1 : 0)
         - 15 * (wallhug_triggered ? 1 : 0)
```

(Exact numbers are tunable; Phase 2 just defines the structure.)

We log **one REWARD event per episode** with:

* `episode_id`
* `reward`
* `blocks_per_minute`
* `total_blocks`
* `duration_seconds`
* flags (`stuck`, `reset`, `wallhug`, `invalid`)

---

## 6. Discretization / Bucketing Strategy

To keep state/action space compact:

### 6.1 Continuous features → buckets

Examples:

* `blocks_per_minute`

  * Buckets like: `[0, 60, 120, 180, 240, 300, 360, 420+]`.
* `mining_ratio`

  * Buckets: `[0.0–0.25, 0.25–0.5, 0.5–0.75, 0.75–1.0]`.
* `y_level`

  * Buckets (just examples): `<20`, `20–40`, `40–80`, `>80`.
* `pitch_pixels`

  * Use equally spaced bins over observed range; e.g. 5–7 bins.
* `sprint_seconds`

  * Bins like: `[0.3–0.5, 0.5–0.7, 0.7–0.9]`.

### 6.2 Discrete IDs

We assign each bucket an **integer ID**, and store:

* Both the **raw continuous value** (for analysis),
* And the **bucket ID** (for learning and indexing).

---

## 7. Database Encoding (Postgres) – High-Level Tables

Top-level tables for telemetry DB (design; implementation Phase 3):

### 7.1 `episodes`

```sql
episodes(
  episode_id          UUID PRIMARY KEY,
  started_at_utc      TIMESTAMPTZ,
  ended_at_utc        TIMESTAMPTZ,
  profile_id          INT,
  profile_name        TEXT,
  pitch_pixels        INT,
  sprint_seconds      REAL,
  strafe_pattern_id   INT,
  lane_execute_flag   BOOL,
  duration_seconds    REAL,
  total_blocks        INT,
  blocks_per_minute   REAL,
  mining_ratio        REAL,
  stuck_flag          BOOL,
  reset_flag          BOOL,
  wallhug_flag        BOOL,
  invalid_flag        BOOL,
  reward              REAL
);
```

### 7.2 `windows`

```sql
windows(
  episode_id          UUID REFERENCES episodes(episode_id),
  window_index        INT,
  window_start_utc    TIMESTAMPTZ,
  window_end_utc      TIMESTAMPTZ,
  ticks               INT,
  mining_ticks        INT,
  block_breaks        INT,
  blocks_per_minute   REAL,
  mining_ratio        REAL,
  stuck_events        INT,
  reset_events        INT,
  low_progress_flag   BOOL,
  wallhug_flag        BOOL,
  -- optional pose/env
  facing_dir_id       INT,
  y_level_bucket      INT,
  lane_index          INT,
  lane_phase_id       INT,
  PRIMARY KEY (episode_id, window_index)
);
```

### 7.3 `state_action_reward`

This is the compact table for learning (one row per **episode** for v0.2):

```sql
state_action_reward(
  episode_id          UUID PRIMARY KEY REFERENCES episodes(episode_id),
  -- state (episode-level summary / first-window-level features)
  blocks_per_min_bucket   INT,
  mining_ratio_bucket     INT,
  y_level_bucket          INT,
  facing_dir_id           INT,
  lane_phase_id           INT,
  -- action
  profile_id              INT,
  pitch_bucket            INT,
  sprint_bucket           INT,
  strafe_pattern_id       INT,
  lane_execute_flag       BOOL,
  -- reward
  reward                  REAL
);
```

Later, we can refine to **window-level SAR** if we want finer-grained RL, but episode-level is plenty for initial self-tuning.

---

## 8. Logging Rules in `app.py` / TelemetryCollector

**Phase 2 outcome** is *design only*, but to guide Phase 3 implementation, we define what we’ll log:

1. **Episode start**

   * Log structured JSON line:

     * `event_type="EPISODE_START"`
     * `episode_id`
     * profile + control params + lane_execute_flag.

2. **Episode end**

   * `event_type="EPISODE_END"`
   * reward fields + flags.

3. **Window summary**

   * Each metrics window or watchdog window:

     * `event_type="WINDOW_SUMMARY"`
     * `episode_id`, `window_index`, performance + env features.

4. **Watchdog / Stuck / Reset events**

   * `event_type="WATCHDOG_LOW_PROGRESS"`, `"WATCHDOG_WALLHUG"`, `"STUCK"`, `"RESET"`.
   * Attach `episode_id` and relevant metrics.

The **TelemetryCollector** parses these structured log lines and populates Postgres tables according to this design.

---

## 9. Next Steps (Phase 3 – Implementation Plan Hooks)

1. **Add structured JSON logs** to `app.py` at:

   * `start_episode()`
   * `end_episode()`
   * watchdog updates
   * metrics window summaries (likely inside `MetricsManager.log_summary` or adjacent).

2. **Extend TelemetryCollector** to:

   * Parse these JSON log entries.
   * Populate `episodes`, `windows`, `state_action_reward`.

3. **Create initial notebook** to:

   * Plot `reward` vs `pitch_bucket` / `sprint_bucket` per profile.
   * Validate that state & reward make sense.

4. **Future (Phase 4)**:

   * Introduce an external learner (Python job / notebook) that reads `state_action_reward` and writes back **updated control_tuning policies** or **strategy profiles**.
   * Wire a simple loop: “every N episodes, adjust the epsilon policy using the telemetry”.

---