````markdown
## Status Update - 2026-03-20

- Live Forge telemetry is now the authoritative runtime state source.
- Bronze, Silver, episode, and Gold pipeline stages are updating from current 2026 runs.
- The old random steering loop has been removed in favor of deterministic movement plus recovery learning.
- Perimeter scout now saves partial traces with derived speed and uses configured-region waypoints instead of stall-only completion.
- Scout leg completion now requires signed progress in the commanded direction, and heading correction has been inverted to match live Forge yaw feedback.
- Scout movement is now fixed to the `/gmine` reference heading and uses relative `W/A/S/D` motion for perimeter tracing instead of rotating at every leg.
- Successful scout reports now emit `recommended_region_bounds`, which can be applied directly to `data/mine_bounds.json`.
- Autonomous mining now logs `NAV toward region` snapshots and uses the corrected autonomous-only heading control path.
- Autonomous mining now detects upward mine-reset teleports, clears stale lane state, and performs a brief top-of-mine reorientation before resuming.
- Autonomous mining now treats the configured region as stone-only at this mine stage: any non-air target block outside `allowed_block_ids` is treated as non-mineable for immediate recovery.
- The Streamlit voxel view is now clipped to the calibrated mine volume and summarizes observed `mineable` stone, `air`, and non-mineable voxels.
- Stop requests are now wired through an interruptible control event so toggle-off can break out of aim/pitch/recovery loops faster.
- Mining hotkeys are now edge-polled from the main loop so `F8`/`F10` do not depend entirely on keyboard callback delivery.
- The Streamlit 3D section now auto-renders the full calibrated mine cube with inferred `unmined` vs observed `mined` cells plus a red player marker.
- The Streamlit cube now reads from the live raw Forge tail and resets to the latest mine cycle instead of waiting on DB compression.
- The live cube trail is now bounded to the current reset cycle with a 5-minute catch-all window.
- A first-pass manual training recorder can now capture human key/mouse inputs alongside Forge telemetry into JSONL sessions.
- Repeated non-mineable materials can now be promoted into a persisted learned blocking list and merged into runtime `blocking_block_ids`.
- Boundary-adjacent non-mineable discoveries are now rendered in the cube even if the target block is just outside the calibrated box.
- The miner now retries in-lane pitch sweeps before lane-shifting, reducing premature drift into a spiral pattern.
- A planning period now selects a run-level mining bias from recent rewarded decision windows before lane mining begins.
- Reset reorientation now accepts top-of-mine teleports a few blocks below the absolute max Y, matching observed resets around `y=150`.
- The current blocker is accurate mine entry from `/gmine`:
  - spawn is around `4863.50 / 152.0 / 16800.50`, facing east
  - the miner can walk to the mine face
  - scout still needs one clean perimeter pass to lock real mine corners

### Immediate Next Steps

1. Stabilize entry from `/gmine`.
   - Walk forward with a level camera.
   - If progress stalls near the configured region, pitch down and mine into the foot-level blocks.
   - Capture the exact coordinate where forward walking first contacts the mine face.

2. Replace inferred bounds with captured bounds.
   - Capture two opposite corners from F3.
   - Update `data/mine_bounds.json`.
   - Keep `config/config.yaml` aligned for docs and future calibration mode.

3. Improve visibility while debugging.
   - Show controller stage: `walk-in`, `entry-mining`, `lane-mining`.
   - Keep showing the latest raw Forge tick when DB-backed live views lag.
   - Derive horizontal speed from consecutive Forge ticks and include it in scout/debug outputs.

4. Add a perimeter-scout calibration pass.
   - Trigger from a dedicated hotkey.
   - Trace the walkable rectangle from `/gmine`.
   - Save the observed perimeter to `data/perimeter_scout_last_run.json`.
   - Preserve partial traces on interrupted runs so every calibration attempt is useful.
   - In `--mode scout`, ignore `F8` while scout is active so calibration cannot accidentally flip into mining.

5. Resume lane-planning and learning only after deterministic entry is stable.
   - Use `O` manual recording sessions as supervised traces for future policy fitting.

# Minecraft Auto Miner – Implementation Plan v0.7.0  
*(2025-12-08 – Telemetry DB, Window Rewards, Forge F3 Telemetry & Calibration)*

---

## 0. Mission & Scope (Updated)

We already have a **working miner core + telemetry DB**:

- Miner:
  - Hotkeys, FSM, `/gmine` recovery.
  - Walks forward, mines, can detect “stuck” / low progress.
- Telemetry:
  - **Postgres DB** (`mam_telemetry`).
  - Tables: `telemetry.episode`, `telemetry.decision_window`, `telemetry.miner_event`.
  - `WINDOW_SUMMARY` rows now have `blocks_broken` + `mining_ratio` and per-window reward placeholders.

What is still missing is the part you actually care about:

> When I arm the miner, it should:
> - Understand **where it is** inside the mineable volume.
> - Know **what block it’s looking at** (stone vs wool vs air).
> - Learn which **FSM actions** (turn, strafe, reset, etc.) keep it in fresh stone.
> - Avoid **already-mined lanes** within the 5-minute reset window.
> - Use window-level rewards to **self-improve** without being disarmed.

### Design Pivot (2025-12-08)

**Old idea (commented out now):**

- Use **MineRL + F3 analysis** as the primary way to understand state and design a “F3 state spec”.

```markdown
<!--
Previous v0.6.0 focus:
- Treat MineRL as primary R&D tool.
- Design F3-like state schema first, then figure out how to bring it back to live client.
This is now optional / future-facing. Live miner needs a more direct integration.
-->
```

**New core decision:**

> Use a **Forge client mod** to emit F3-like telemetry directly from the live client  
> (position, yaw/pitch, block-under-crosshair) to a local JSON log,  
> and consume that in Python as the primary world state


MineRL is now **optional (preferred not to use) R&D**. Forge telemetry is the **live, real-time state source**.

---

## 1. Current State Snapshot (2025-12-08)

### 1.1 Code & Files

Full version in attached Minecraft_auto_miner.tree.json file. 

```text
E:\Minecraft_auto_miner
  Implementation_plan.md
  README.md
  pyproject.toml
  config\
    config.yaml
  data\
    control_tuning.json
    strategy_stats.json
  logs\
    mining_helper.log
  src\
    minecraft_auto_miner\
      app.py            # v0.5.7 – telemetry JSON + basic learning hooks
      telemetry\
        telemetry_collector.py  # v0.6.3 – DB writer from log
      ...
      world_model.py
      pose.py
      movement_primitives.py
      lane_planner.py
      lane_controller.py
      ...
```

### 1.2 DB & Telemetry

- **Postgres DB**: `mam_telemetry`
- Tables:
  - `telemetry.episode` – episodes are being inserted.
  - `telemetry.decision_window` – window-level rows exist with `blocks_broken`, `mining_ratio`, `reward`.
  - `telemetry.miner_event` – events like TELEMETRY_JSON, STUCK_EVENT, etc.

Example (recent):

```text
window_id, episode_id, profile_name, start_ts, end_ts, state_code, action_code, reward, blocks_broken, mining_ratio
31,27,straight_lane_default,2025-12-08 21:46:05+00:00,2025-12-08 21:46:05+00:00,1,1,0,1,1
30,27,straight_lane_default,2025-12-08 21:45:35+00:00,2025-12-08 21:46:05+00:00,1,1,0,357,1
...
```

**Interpretation:**

- Windows capture **block breaks** and mining ratio, but:
  - reward is still `0` (not yet wired to blocks/min).
  - `state_code`/`action_code` are simple buckets, not yet tied to a meaningful FSM decision loop.

### 1.3 Status by Layer

**Layer 1 – Motor & Perception (v0.4.x)** – ✅ *Complete & working baseline*

- FSM, InputController, Perception (audio), reset/stuck detectors.
- `/gmine` recover works, miner can dig lanes in a straight line.

**Layer 2 – Telemetry & World Model (v0.5.x / v0.6.x)** – 🟡 *Partially implemented*

- ✅ `TelemetryCollector` tails `mining_helper.log` and writes to Postgres.
- ✅ TELEMETRY_JSON events from `app.py`:
  - EPISODE_START, WINDOW_SUMMARY, EPISODE_END, RESET_EVENT, STUCK_EVENT, WATCHDOG_*.
- ✅ `telemetry.decision_window` rows populated with:
  - `episode_id`, `start_ts`, `end_ts`, `blocks_broken`, `mining_ratio`, `state_code`, `action_code`, `reward`.
- 🟡 World model + lanes exist but:
  - No *reliable* notion of “where we are” yet.
  - No awareness of **wool boundary** vs **stone interior** beyond audio/mining ratio.

**Layer 3 – Self-Learning Policy** – 🔴 *Not effective yet*

- `policy_bootstrap` summarizes runs into `strategy_stats.json`.
- Profiles (`straight_lane_default`, `straight_sprint`, `strafe_slight`) exist, but:
  - Miner still behaves like “sprint forward & mine” with little/no useful variation.
  - No real “explore vs exploit” loop tied to **window-level rewards**.
  - No understanding of wool barriers or “already mined” regions.

---

## 2. High-Level Architecture (Updated with Forge)

```text
+--------------------------------------------------------------+
| LAYER 3: SELF-LEARNING FSM & POLICY                          |
| - Window-level decisions using Q-table / bandit              |
| - Chooses actions: turn/strafe/reset/lane-change             |
| - Reads rewards from telemetry DB                            |
+--------------------------------------------------------------+
| LAYER 2: TELEMETRY & WORLD MODEL                             |
| - Postgres: episode, decision_window, miner_event            |
| - Forge telemetry: x,y,z,yaw,pitch,target_block_id           |
| - Calibration: discover bounds, build lane map               |
| - Track "already mined" regions within 5 min reset           |
+--------------------------------------------------------------+
| LAYER 1: MOTOR & PERCEPTION                                  |
| - Existing FSM, audio-based perception                       |
| - reset_detector, stuck_detector, watchdogs                  |
| - Profiles + control_tuning                                  |
+--------------------------------------------------------------+
```

Key updates:

- **Forge mod** becomes the **canonical pose source**.
- Audio’s `has_block_break` and mining ratio remain as secondary signals.
- Lane/world model now anchored in **real coordinates**, not dead-reckoning guesses.

---

## 3. Design Pivot: Forge-Based F3 Telemetry

### 3.1 Rationale

Problems with current approach:

- We **can’t tell stone vs wool** reliably from audio alone.
- Miner:
  - Mines straight into wool boundaries.
  - Keeps mining “invalid” blocks instead of turning or resetting.
- Dead-reckoning pose is too fragile for a 3D cube + 5 minute reset.

Forge solution:

- **Client-only mod**:
  - Every client tick, write JSON:
    - `x, y, z`
    - `yaw, pitch`
    - `dimension`
    - `target_block_id` & `target_x, target_y, target_z`
    - simple flags like `is_sprinting`, `on_ground`
  - Append to: `.minecraft/mam_telemetry/mam_f3_stream.log`.

- **Python**:
  - `ForgePoseProvider` tails the log.
  - Supplies `Pose` + `TargetBlock` to:
    - Lane planner.
    - Calibration logic.
    - Barrier detection (wool vs stone).
    - Learning layer.

### 3.2 Consequences for the Plan

- **Old MineRL-first exploration** is now **optional / future** (kept for R&D):
  - Might still be used for simulation / offline training.
  - Not required to get a self-learning miner running in your actual server.

- **New core path**:
  1. Implement Forge mod.
  2. Implement `ForgePoseProvider` in Python.
  3. Add a **calibration phase** that explores boundaries and commands.
  4. Drive lane/world model + learning FSM purely from Forge state + telemetry DB.

---

## 4. Phase Map (With Status)

Legend:  
✅ Done 🟡 In progress 🔴 Not done

1. **Phase 0 – Baseline & Telemetry DB** – ✅ DONE
2. **Phase 1 – Window Rewards & Barrier Signals** – 🟡 PARTIAL
3. **Phase 2 – Forge Telemetry Integration** – 🔴 TODO
4. **Phase 3 – Calibration Runs & Boundary Map** – 🔴 TODO
5. **Phase 4 – Self-Learning FSM & Action Application** – 🔴 TODO
6. **Phase 5 – MineRL/Sim R&D (Optional)** – 🔴 LATER

Below we rewrite the phases in their v0.7.0 form.

---

## 5. Phase 0 – Baseline & Telemetry DB (✅ Completed)

**Goal:** Lock in “it works” miner and get structured telemetry into DB.

### 5.1 Miner Baseline

- Keep a git tag:
  - `v0.5.7_baseline_telemetry` or similar.
- Guarantee:
  - `/gmine` works on startup and on resets.
  - Miner can dig lanes in a straight line.
  - Panic stop always stops everything.

### 5.2 Telemetry JSON & DB (Already Implemented)

- `app.py`:
  - Emits TELEMETRY_JSON:
    - `EPISODE_START`
    - `WINDOW_SUMMARY` (per metrics window)
    - `EPISODE_END`
    - `RESET_EVENT`, `STUCK_EVENT`, `WATCHDOG_*`
- `telemetry_collector.py`:
  - Parses log lines.
  - Inserts into:
    - `telemetry.episode`
    - `telemetry.decision_window`
    - `telemetry.miner_event`
  - Uses `state_code`, `action_code`, `reward` fields for each window.

**→ No changes required here; this is the stable foundation.**

---

## 6. Phase 1 – Window Rewards & Barrier Signals (🟡 Partial)

**Goal:** Make `decision_window.reward`, `state_code`, `action_code` actually meaningful, especially around **wool barriers** vs **stone lanes**.

Right now:

- `blocks_broken` is populated, but reward is effectively **0**.
- `state_code` is a simple bucket of blocks/min.
- We still **don’t know** whether the blocks broken are *stone* or *wool*.

### 6.1 Reward Model (Short-Term, Pre-Forge)

Even before Forge, we can improve reward:

- Reward per window:
  ```text
  reward = blocks_per_minute
         - penalty_low_progress
         - penalty_stuck_or_reset
  ```
- Map into `telemetry.decision_window.reward` via the telemetry collector OR by computing it in `policy_bootstrap` and backfilling.

**Completion for v0.7.0 short term:**

- [ ] Ingest `blocks_per_minute` into `decision_window.reward`
- [ ] Add penalties for:
  - stuck episodes
  - low-progress watchdogs
  - resets

This sets the stage for the learning FSM, even if it doesn’t yet know about wool vs stone.

### 6.2 Barrier Signals (Concept Only – Needs Forge)

Once Forge telemetry is in place:

- For each tick/window we can compute:
  - `stone_hits`: count of ticks where `target_block_id` is stone and audio indicates a break.
  - `wool_hits`: count when `target_block_id` is wool but audio still thinks it’s breaking (or we see repeated wool).
- Update `state_code` to consider:
  - “Dominant block type under crosshair” (stone / wool / air).
  - “Recently hit barrier” flag.

These features then drive the FSM and learning policy in later phases.

---

## 7. Phase 2 – Forge Telemetry Integration (🔴 TODO)

**Goal:** Replace dead-reckoning with **actual position + target block** via Forge.

### 7.1 Forge Mod (Java, Client Only)

Deliverable: `mam_forge_telemetry` mod that:

- Hooks **client tick**.
- Collects:
  - Player position: `x, y, z`
  - Rotation: `yaw, pitch`
  - Dimension: `level.dimension().location()`
  - Crosshair block:
    - `target_block_id` (e.g. `"minecraft:stone"`, `"minecraft:white_wool"`)
    - `target_x, target_y, target_z`
- Writes one JSON line per tick to:
  - `.<minecraft>/mam_telemetry/mam_f3_stream.log`

Status:

- Design sketched.
- Implementation not yet built/tested.

Tasks:

- [ ] Create Forge mod project (matching your MC version).
- [ ] Implement tick handler + JSON writer.
- [ ] Verify log file path & contents while moving around / mining.

### 7.2 `ForgePoseProvider` in Python

Deliverable: `forge_pose_provider.py` that:

- Tails `mam_f3_stream.log`.
- Maintains latest:
  - `Pose(x, y, z, facing)` (converted from yaw).
  - `TargetBlock(block_id, x, y, z)` or `None`.
- API:
  - `get_pose() -> Pose | None`
  - `get_target_block() -> TargetBlock | None`

Integration with `app.py`:

- New config section:

  ```yaml
  forge_telemetry:
    enabled: true
    log_path: "C:/Users/<YOU>/AppData/Roaming/.minecraft/mam_telemetry/mam_f3_stream.log"
  ```

- If `forge_telemetry.enabled`:
  - Use `ForgePoseProvider` instead of `DeadReckoningPoseProvider`.
  - Pass this into lane/world model and calibration logic.

Tasks:

- [ ] Implement `ForgePoseProvider`.
- [ ] Wire it in as pose provider when enabled.
- [ ] On shutdown, ensure the provider thread is stopped cleanly.

**Completion criteria:**

- While the miner runs, debugging logs show:
  - Pose updates that match in-game movement.
  - Target block ID that matches what you’re looking at (stone vs wool).

---

## 8. Phase 3 – Calibration Runs & Boundary Map (🔴 TODO)

**Goal:** Add a **calibration phase** where the miner learns the mineable cube boundaries and what it is allowed to do, then saves this into a profile.

### 8.1 Calibration Concepts

Constraints:

- Mineable area resets every ~5 minutes.
- Stone inside, wool/air/other at boundaries.
- Once `minecraft:stone` is mined, it’s **not mineable** again until reset.

We want the miner to:

1. Discover approximate bounding box `(x_min, x_max, z_min, z_max, y_lane)` at the current layer.
2. Verify that a given **command set** (forward, strafe, yaw ±90°, sprint) keeps it within safe area.
3. Store:
   - `mine_bounds` (per-profile, per-world).
   - Lane layout derived from those bounds.

### 8.2 Calibration Procedure (Outline)

New FSM mode: `CALIBRATION`.

High-level steps:

1. **Spawn and initial `/gmine`** (existing behaviour).
2. Use Forge pose + target block to:
   - Move forward until target block becomes non-stone/wool boundary.
   - Record that x/z coordinate as a boundary.
3. Back up / turn 180° and repeat in opposite direction.
4. Strafe left/right to probe perpendicular boundaries.
5. Build a coarse grid:

   ```python
   mine_bounds = {
       "x_min": ...,
       "x_max": ...,
       "z_min": ...,
       "z_max": ...,
       "y_lane": current_y_layer
   }
   ```

6. Save to disk:
   - `data/mine_bounds.json` keyed by:
     - dimension
     - server name / profile

### 8.3 Already-Mined Map Within 5-Minute Reset

Within the known bounds:

- Maintain an in-memory map (coarse):

  ```python
  mined_voxels[(x, y, z)] = last_mined_ts
  ```

- During a 5-minute cycle:
  - If `target_block_id` is stone and we see a block break:
    - Mark that (x, y, z) as mined.
  - When planning a new lane:
    - Prefer segments that contain unmined voxels.
- On reset:
  - Clear the map.
  - Or lazily clear entries older than 5 minutes.

Tasks:

- [ ] Implement calibration FSM mode + simple pathing to detect boundaries.
- [ ] Write `mine_bounds` JSON after calibration completes.
- [ ] Implement `mined_voxels` map and simple query API for “is this lane worth it?”.

**Completion criteria:**

- After calibration, logs show computed `mine_bounds`.
- Miner can be restarted and re-use `mine_bounds` to know its cube limits.
- Within a 5-minute cycle, it avoids clearly “dead” lanes (all wool / already mined).

---

## 9. Phase 4 – Self-Learning FSM & Action Application (🔴 TODO)

**Goal:** Turn window-level metrics and Forge state into **real decisions** that change behaviour and improve throughput.

### 9.1 FSM States (Updated)

Core high-level states:

1. `WALK_AND_MINE`
2. `HIT_BARRIER` (target wool / no new stone breaks)
3. `RECOVER_TURN` (rotate ±90°)
4. `RECOVER_STRAFE` (shift into new lane)
5. `RECOVER_GMINE` (full reset)
6. `CALIBRATION` (Phase 3)
7. `IDLE` / `PANIC_STOP`

Barriers are now **explicit transitions**:

- If:
  - `target_block_id` is wool OR
  - no new stone voxels for a window, but wool target is constant
- Then:
  - Enter `HIT_BARRIER` and **force a decision** among:
    - turn left, turn right, strafe left, strafe right, `/gmine`.

### 9.2 Action Space for Learning

Define a small, meaningful action set:

- `CONTINUE_SAME` – keep mining forward.
- `TURN_LEFT_90`
- `TURN_RIGHT_90`
- `STRAFE_LEFT_1`
- `STRAFE_RIGHT_1`
- `RESET_GMINE_AND_REALIGN` (expensive)
- `TOGGLE_SPRINT` (if we want to learn sprint usage)

Each action is tied to:

- A specific sequence of key presses / camera primitives.
- A known cost (e.g., `/gmine` is slow).

Implementation:

- `miner_actions.apply(action_name)` in `app.py`:
  - Uses existing movement_primitives, camera, and chat macros.
  - Updates internal state (pose, lane index, etc.).

### 9.3 Learning Loop (Window-Level)

Per decision window (already being logged):

- Collect from DB / live metrics:
  - `state_code` (encoded from:
    - blocks/min,
    - mining_ratio,
    - barrier flags,
    - “fresh vs already-mined lane” flag).
  - `action_code` (action taken at start of the window).
  - `reward` (blocks/min – penalties).

- Maintain a Q-table:

  ```python
  Q[state_code][action_code] -> float
  ```

- Update after each window:

  ```python
  Q[s][a] = Q[s][a] + α * (r + γ * max_a' Q[s_next][a'] - Q[s][a])
  ```

- Action choice (ε-greedy):

  ```python
  if random() < ε:
      action = random_action()
  else:
      action = argmax_a Q[state][a]
  ```

Persistence:

- Store Q-table in `data/q_table.json` or a dedicated DB table.
- Load on startup so learning continues across sessions.

Tasks:

- [ ] Implement `state_code` encoding that includes:
  - performance bins (blocks/min),
  - barrier signal,
  - “is_lane_fresh” flag (from mined map),
  - simple orientation bins (from yaw/pitch).
- [ ] Implement `action_code` mapping and `miner_actions`.
- [ ] Implement Q-table + ε-greedy policy.
- [ ] Wire into main loop so that:
  - On hitting a barrier or at fixed intervals, we pick and apply an action.

**Completion criteria:**

- Over time:
  - Action distribution shifts away from “always forward”.
  - Average reward (blocks/min minus penalties) improves.
  - Miner starts reliably turning/strafeing instead of grinding wool.

---

## 10. Phase 5 – MineRL / Sim R&D (Optional / Future)

**Goal:** Use MineRL or other environments for **offline experimentation**, not as a blocker for live behaviour.

Plan (commented / optional):

```markdown
<!--
- Set up a MineRL environment with similar mineable volumes.
- Use the same state/action encoding and Q-table.
- Let an agent train in simulation until it discovers good recovery policies.
- Extract heuristics or pre-trained Q-table and transfer to live miner.

This is useful but NOT required to get a performant live system.
-->
```

---

## 11. Versioning & Workflow

- Implementation plan version: **v0.7.0**.
- Code version headers:
  - `app.py` → `v0.7.x` once Forge + calibration start landing.
  - `telemetry_collector.py` remains `v0.6.x` until schema changes.

Suggested git tags:

- `v0.6.3_telemetry_db_live` – what you have now.
- `v0.7.0_forge_pose_provider` – Forge integration.
- `v0.7.1_calibration_bounds_map`
- `v0.7.2_self_learning_fsm_v1`

Use your existing `/tree` snapshots and sync-tree workflow to keep the structure visible and versioned.

---

## 12. Concrete Next Steps (Dev Checklist)

Short, brutally direct list so the **next run is not “same shit”**:

1. **Forge mod:**  
   - [ ] Build & test `mam_forge_telemetry` mod → verify JSON log while walking and looking at stone vs wool.

2. **ForgePoseProvider:**  
   - [ ] Implement `forge_pose_provider.py`.  
   - [ ] Wire into `app.py` when `forge_telemetry.enabled = true`.  
   - [ ] Log pose + `target_block_id` each second for sanity.

3. **Barrier-aware state:**  
   - [ ] Add a simple “barrier hit” heuristic based on:
     - `target_block_id` is wool AND blocks/min dropping.
   - [ ] Log when we transition into a `HIT_BARRIER` state.

4. **Calibration stub:**  
   - [ ] Add a manual “CALIBRATE” mode (e.g. a hotkey or config flag) that:
     - Walks forward + logs pose and target until hitting wool.
     - Writes a crude `mine_bounds.json` for inspection.

Once those are in place, we can tighten the calibration logic and hook in the learning FSM.

---

*(End of Implementation Plan v0.7.0 – Forge Telemetry, Calibration & Self-Learning FSM)*
````
