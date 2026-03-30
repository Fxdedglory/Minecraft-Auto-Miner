# Minecraft Auto Miner

Forge-telemetry-driven Minecraft prison miner with:
- calibrated mouse + movement control
- deterministic lane mining inside configured bounds
- persistent scout and voxel memory
- telemetry-backed flight-burst validation
- Streamlit operator dashboard
- live telemetry ingest into Postgres

## Current State

The project now operates as a telemetry-first autonomous miner:
- `control calibration` measures real yaw, pitch, walk, sprint, jump, crouch, and flight behavior before reuse
- `perimeter scout` learns the mine border and persists reusable border memory
- `mine mode` uses configured bounds, scout memory, voxel memory, and calibrated controls together
- `flight tunnel bursts` are validated from telemetry during runtime instead of being assumed successful
- `mine reset` clears remembered mineable voxels while preserving discovered border and non-mineable memory
- `runtime_console_latest.log` mirrors the current run so debugging does not require pasting terminal output every time

The system is intentionally hybrid:
- geometry, safety, and obvious world rules are hand-authored
- learning is used for pattern choice, recovery preference, and long-run improvement
- telemetry is the source of truth when deciding whether a strategy really worked

## Operator Workflow

1. Start Minecraft and enter the mine world.
2. Run `python -m minecraft_auto_miner.app`
3. Use `F8` to start or stop mining
4. Use `F10` to start or stop perimeter scout
5. Use `O` to start or stop manual recording
6. Use `F9` for panic stop

Optional launch modes:
- `python -m minecraft_auto_miner.app --mode mine`
- `python -m minecraft_auto_miner.app --mode scout`
- `python -m minecraft_auto_miner.app --mode calibrate`

Forge log helper:
- `python -m minecraft_auto_miner.app --rotate-forge-log`
- `python -m minecraft_auto_miner.app --rotate-forge-log --rotate-forge-log-keep-mb 8`

## Runtime Outputs

Important runtime files:
- [runtime_console_latest.log](/e:/Minecraft_auto_miner/data/runtime_console_latest.log)
  Overwritten on each run and mirrors console output.
- [control_calibration_last_run.json](/e:/Minecraft_auto_miner/data/control_calibration_last_run.json)
  Latest calibration result and segment telemetry summary.
- [perimeter_scout_last_run.json](/e:/Minecraft_auto_miner/data/perimeter_scout_last_run.json)
  Latest scout trace and observed bounds.
- [voxel_world_memory.json](/e:/Minecraft_auto_miner/data/voxel_world_memory.json)
  Persistent world-state memory used by mining and the cube view.
- [strategy_stats.json](/e:/Minecraft_auto_miner/data/strategy_stats.json)
  Runtime pattern outcomes, episode summaries, and exploit-vs-explore statistics.

## Dashboard

The Streamlit dashboard is arranged for hands-off monitoring while Minecraft owns mouse and keyboard:
1. `Minecraft Auto Miner - Controls & Telemetry`
2. `Literal Mine Cube`
3. Literal Mine Cube plot
4. `Runtime Controls`
5. Runtime control buttons

The top cube refreshes independently so the control area should stay clickable.

## What The Miner Knows

The miner combines several kinds of memory:
- configured region bounds from [mine_bounds.json](/e:/Minecraft_auto_miner/data/mine_bounds.json)
- learned blocking materials from [learned_blocking_block_ids.json](/e:/Minecraft_auto_miner/data/learned_blocking_block_ids.json)
- reusable scout memory from [perimeter_scout_memory.json](/e:/Minecraft_auto_miner/data/perimeter_scout_memory.json)
- live voxel memory from [voxel_world_memory.json](/e:/Minecraft_auto_miner/data/voxel_world_memory.json)
- runtime strategy outcomes from [strategy_stats.json](/e:/Minecraft_auto_miner/data/strategy_stats.json)

This is the intended hierarchy:
- bounds decide where mining is even allowed
- scout explains borders and known non-mineable structure
- voxel memory tracks observed mineable, mined-air, and barrier cells
- telemetry validates whether a chosen mining pattern actually performed as intended

## Main Components

- [app.py](/e:/Minecraft_auto_miner/src/minecraft_auto_miner/app.py)
  Main miner loop, calibration, scout, mining controller, flight validation, pattern scoring, runtime log capture, and Forge log helper.
- [main_streamlit.py](/e:/Minecraft_auto_miner/src/minecraft_auto_miner/dashboard/main_streamlit.py)
  Operator dashboard and top-pinned Literal Mine Cube.
- [fsm_event_log.py](/e:/Minecraft_auto_miner/src/minecraft_auto_miner/telemetry/fsm_event_log.py)
  High-level FSM event logging into telemetry storage.
- [bronze_f3_ingest.py](/e:/Minecraft_auto_miner/src/minecraft_auto_miner/telemetry/bronze_f3_ingest.py)
  Tail-based raw Forge ingest.
- [silver_f3_compress.py](/e:/Minecraft_auto_miner/src/minecraft_auto_miner/telemetry/silver_f3_compress.py)
  Bronze to Silver compression.
- [episodes_from_silver.py](/e:/Minecraft_auto_miner/src/minecraft_auto_miner/telemetry/episodes_from_silver.py)
  Episode and decision-window construction.

## Debug Checklist

If a run looks wrong, check these in order:
- Is [runtime_console_latest.log](/e:/Minecraft_auto_miner/data/runtime_console_latest.log) updating?
- Is the Forge log still growing at `C:\Users\gamer\AppData\Roaming\.minecraft\mam_telemetry\mam_f3_stream.log`?
- Does startup show the expected region bounds and calibration profile load?
- If flight is expected, does the log show `Flight tunnel telemetry validator...` or a `Pattern execution result` for `flight_tunnel_burst`?
- If scout memory should exist, does startup show `Loaded scout mining memory...`?
- If a reset happened, did mined-air disappear from the cube while the border remained?
- If the bot is stuck, are there repeated `Lane drift correction`, `Strict local stone seek`, or `Strict soak protection triggered` lines?

## Known Remaining Work

The project is close to “full-time use,” but these are still worth improving:
- more lane-center recovery using path history before camera seek
- deeper use of scout + voxel memory for choosing the next productive direction
- continued tuning of smooth 90 / 180 degree turn profiles during flight mining
- final dead-file cleanup once behavior is stable enough that we are no longer iterating on rollback points

## Lessons Learned

These are the reusable ideas from this project that should transfer well to future automation work:

### 1. Use telemetry as the source of truth

Do not trust intended inputs.
Trust measured outputs.

Examples from this miner:
- a double-jump does not prove flight mode actually enabled
- a turn command does not prove the camera ended at the target heading
- a reset command does not prove the player landed at the top-entry pose

The robust pattern is:
1. issue an input
2. measure pose / target / speed / heading change
3. classify the attempt from telemetry
4. only reuse the behavior if telemetry says it actually worked

### 2. Separate rules from learning

Learning should not rediscover facts that are stable and obvious:
- Minecraft blocks are axis-aligned cubes
- border blocks stay borders across resets
- outside configured bounds is not mineable
- a reset changes mineable state but not border geometry

What learning should choose:
- whether flight or ground is better in a given tunnel context
- which recovery macro is best after a stall
- how aggressively to shift, reacquire, or reset

### 3. Build deterministic baselines first

Before exploration:
- calibrate controls
- establish deterministic mining lanes
- persist scout memory
- make reset handling correct

Only after that should exploration pick between macros or strategy variants.

### 4. Score episodes at the level the system actually operates

For this miner, the meaningful unit is a mine-reset cycle.
That means strategy should be scored by:
- productive distance / blocks mined
- time lost to stalls
- time lost to drift correction
- successful versus failed flight bursts
- number of resets and recoveries

This is much better than rewarding arbitrary single ticks.

### 5. Prefer reusable macros over raw-action exploration

Good macro examples:
- straight flight tunnel burst
- ground forward tunnel
- local lane transition
- drift recovery
- reset and re-entry

This keeps learning interpretable and safe.

### 6. Design operator tools around real use

The dashboard mattered more once the bot became autonomous.
The best operator UX choices were:
- top-pinned live cube
- runtime controls near the cube
- runtime log mirrored to a file
- partial scout / calibration artifacts saved during runs

## Experiment Design Template

For similar future projects, use this loop:

1. Define the environment state you can measure reliably.
   Pose, heading, target object, speed, timestamps, and any derived state.
2. Define a tiny deterministic baseline controller.
   No learning at first.
3. Add a calibration harness.
   Prove the controls do what you think they do.
4. Add runtime validators.
   Re-check important behaviors during real execution.
5. Add persistent memory.
   World memory, task memory, and episode memory should be distinct.
6. Score outcomes at the episode level.
   Choose a score tied to the real product goal.
7. Add controlled exploration.
   Mostly use the best-known macro, occasionally test a nearby variant.
8. Add soak protection.
   Long unattended runs need loop detection and fast escalation.
9. Improve the operator console.
   Make debugging possible without interrupting the controlled application.

## Notes For Future Rebuilds

If rebuilding something like this from scratch:
- start with a logger and a replayable telemetry file format first
- add a tiny visualization early
- keep the controller explicit and state-based
- persist every important calibration and runtime artifact
- never merge “control correctness” and “strategy learning” into one blob

That separation is what made this project recoverable while it was still changing quickly.
