"""
strategies.py v0.6.0 – 2025-12-08
Simplified epsilon-greedy strategy manager for Minecraft Auto Miner.

Goals:
- Keep a small set of per-profile stats (runs, duration, blocks, mining ratio).
- Use an epsilon-greedy policy over average blocks/minute.
- Ensure each profile gets at least `exploration_runs_per_profile` episodes.
- Provide a clean, predictable API for app.py:
    * StrategyManager(..., config_mapping)
    * .config (StrategyConfig)
    * .get_stats_snapshot() -> Dict[str, ProfileStats]
    * .on_episode_end(episode_stats, profile, valid=True)
    * .select_next_profile() -> MiningProfile
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List
import logging
import random

from ..src.minecraft_auto_miner.metrics import WindowStats
from .mining_profiles import MiningProfile


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class ProfileStats:
    runs: int = 0
    total_duration_seconds: float = 0.0
    total_blocks: int = 0
    total_mining_ratio: float = 0.0

    avg_blocks_per_minute: float = 0.0
    avg_mining_ratio: float = 0.0

    def update_from_episode(self, episode_stats: WindowStats) -> None:
        """
        Update stats based on a single episode aggregate WindowStats.

        We assume:
          - episode_stats.duration_seconds
          - episode_stats.block_breaks
          - episode_stats.blocks_per_minute
          - episode_stats.ticks
          - episode_stats.mining_ticks
        are available (see metrics.WindowStats).
        """
        self.runs += 1

        duration = float(getattr(episode_stats, "duration_seconds", 0.0))
        blocks = int(getattr(episode_stats, "block_breaks", 0))
        ticks = int(getattr(episode_stats, "ticks", 0))
        mining_ticks = int(getattr(episode_stats, "mining_ticks", 0))

        self.total_duration_seconds += max(duration, 0.0)
        self.total_blocks += max(blocks, 0)

        # Mining ratio for this episode
        if ticks > 0:
            episode_mining_ratio = float(mining_ticks) / float(ticks)
        else:
            episode_mining_ratio = 0.0

        self.total_mining_ratio += episode_mining_ratio

        # Derived averages
        if self.total_duration_seconds > 0.0:
            self.avg_blocks_per_minute = (
                self.total_blocks / self.total_duration_seconds
            ) * 60.0
        else:
            self.avg_blocks_per_minute = 0.0

        if self.runs > 0:
            self.avg_mining_ratio = self.total_mining_ratio / float(self.runs)
        else:
            self.avg_mining_ratio = 0.0


@dataclass
class StrategyConfig:
    auto_switch_enabled: bool = True

    min_episode_duration_seconds: float = 30.0
    min_valid_blocks: int = 10

    exploration_runs_per_profile: int = 2
    epsilon: float = 0.15

    weight_blocks_per_minute: float = 1.0
    weight_mining_ratio: float = 0.0

    auto_lock_best_profile: bool = False
    min_total_valid_episodes_for_lock: int = 12
    min_profile_valid_episodes_for_lock: int = 3


# ---------------------------------------------------------------------------
# Strategy manager
# ---------------------------------------------------------------------------


class StrategyManager:
    """
    Epsilon-greedy profile selector.

    Behaviour:
      - For the first N runs per profile (exploration_runs_per_profile),
        we *ensure* each profile gets sampled.
      - After that, we compute a score:

            score = w_bpm * avg_blocks_per_minute
                    + w_ratio * avg_mining_ratio

        and use epsilon-greedy over that score.

      - If auto_lock_best_profile is enabled and we have enough total
        valid episodes, we always pick the current best profile.

    All state is maintained in-memory, but can be serialised/deserialised
    via get_stats_snapshot + the helper functions in app.py.
    """

    def __init__(
        self,
        profiles: List[MiningProfile],
        logger: logging.Logger,
        config_mapping: Dict[str, object] | None = None,
    ) -> None:
        self._logger = logger.getChild("strategy")

        if not profiles:
            raise ValueError("StrategyManager requires at least one MiningProfile.")

        self._profiles: List[MiningProfile] = list(profiles)
        self._profile_stats: Dict[str, ProfileStats] = {
            p.name: ProfileStats() for p in self._profiles
        }

        raw = config_mapping or {}
        self.config = StrategyConfig(
            auto_switch_enabled=bool(raw.get("auto_switch_enabled", True)),
            min_episode_duration_seconds=float(
                raw.get("min_episode_duration_seconds", 30.0)
            ),
            min_valid_blocks=int(raw.get("min_valid_blocks", 10)),
            exploration_runs_per_profile=int(
                raw.get("exploration_runs_per_profile", 2)
            ),
            epsilon=float(raw.get("epsilon", 0.15)),
            weight_blocks_per_minute=float(
                raw.get("weight_blocks_per_minute", 1.0)
            ),
            weight_mining_ratio=float(raw.get("weight_mining_ratio", 0.0)),
            auto_lock_best_profile=bool(raw.get("auto_lock_best_profile", False)),
            min_total_valid_episodes_for_lock=int(
                raw.get("min_total_valid_episodes_for_lock", 12)
            ),
            min_profile_valid_episodes_for_lock=int(
                raw.get("min_profile_valid_episodes_for_lock", 3)
            ),
        )

        self._logger.info(
            "StrategyManager initialised with %d profiles, "
            "epsilon=%.3f, exploration_runs_per_profile=%d, "
            "weight_blocks_per_minute=%.2f, weight_mining_ratio=%.2f, "
            "auto_switch_enabled=%s, auto_lock_best_profile=%s",
            len(self._profiles),
            self.config.epsilon,
            self.config.exploration_runs_per_profile,
            self.config.weight_blocks_per_minute,
            self.config.weight_mining_ratio,
            self.config.auto_switch_enabled,
            self.config.auto_lock_best_profile,
        )

    # ------------------------------------------------------------------
    # Public API used by app.py
    # ------------------------------------------------------------------

    def get_stats_snapshot(self) -> Dict[str, ProfileStats]:
        """
        Return a shallow copy of the stats dict for persistence.

        app.py / telemetry_inspect use this to save/load state.
        """
        return dict(self._profile_stats)

    def on_episode_end(
        self,
        episode_stats: WindowStats,
        profile: MiningProfile,
        valid: bool = True,
    ) -> None:
        """
        Update stats after an episode.

        app.py already decides when an episode is "valid" for learning
        (min duration, min blocks, etc). We just honour that flag.
        """
        if not valid:
            self._logger.debug(
                "on_episode_end: episode for profile='%s' marked invalid; skipping.",
                profile.name,
            )
            return

        stats = self._profile_stats.get(profile.name)
        if stats is None:
            stats = ProfileStats()
            self._profile_stats[profile.name] = stats

        stats.update_from_episode(episode_stats)

        self._logger.info(
            "Updated stats for profile='%s': runs=%d, avg_blocks_per_minute=%.2f, "
            "avg_mining_ratio=%.3f, total_blocks=%d, total_duration=%.1fs",
            profile.name,
            stats.runs,
            stats.avg_blocks_per_minute,
            stats.avg_mining_ratio,
            stats.total_blocks,
            stats.total_duration_seconds,
        )

    # ------------------------------------------------------------------
    # Profile selection logic
    # ------------------------------------------------------------------

    def _score_profile(self, name: str) -> float:
        stats = self._profile_stats.get(name)
        if stats is None:
            return 0.0

        return (
            self.config.weight_blocks_per_minute * stats.avg_blocks_per_minute
            + self.config.weight_mining_ratio * stats.avg_mining_ratio
        )

    def _total_valid_runs(self) -> int:
        return sum(p.runs for p in self._profile_stats.values())

    def _best_profile_by_score(self) -> MiningProfile:
        best_profile = self._profiles[0]
        best_score = self._score_profile(best_profile.name)
        best_runs = self._profile_stats[best_profile.name].runs

        for p in self._profiles[1:]:
            s = self._score_profile(p.name)
            runs = self._profile_stats[p.name].runs
            # Prefer higher score; tie-break by fewer runs to keep exploration,
            # then by name for determinism.
            if (
                s > best_score
                or (s == best_score and runs < best_runs)
                or (s == best_score and runs == best_runs and p.name < best_profile.name)
            ):
                best_profile = p
                best_score = s
                best_runs = runs

        return best_profile

    def select_next_profile(self) -> MiningProfile:
        """
        Core selection function used by app.py.

        Policy:
          1) If any profile has runs < exploration_runs_per_profile,
             randomly pick from that subset (forced exploration).
          2) Else, if auto_lock_best_profile is enabled and we have
             enough episodes, always return the current best.
          3) Else, epsilon-greedy w.r.t score(profile).

        Note: app.py is now responsible for *always* switching to the
        profile returned here (no extra auto_switch_enabled gate).
        """
        cfg = self.config

        # 1) Forced exploration: ensure we see each profile at least N times.
        exploration_candidates: List[MiningProfile] = [
            p
            for p in self._profiles
            if self._profile_stats[p.name].runs < cfg.exploration_runs_per_profile
        ]
        if exploration_candidates:
            chosen = random.choice(exploration_candidates)
            self._logger.info(
                "StrategyManager: forced exploration, choosing profile='%s' "
                "(runs=%d < exploration_runs_per_profile=%d).",
                chosen.name,
                self._profile_stats[chosen.name].runs,
                cfg.exploration_runs_per_profile,
            )
            return chosen

        total_runs = self._total_valid_runs()

        # 2) Optional lock-on-best behaviour (not super important yet, but wired).
        if (
            cfg.auto_lock_best_profile
            and total_runs >= cfg.min_total_valid_episodes_for_lock
        ):
            best_profile = self._best_profile_by_score()
            self._logger.info(
                "StrategyManager: auto-lock best profile='%s' after %d episodes.",
                best_profile.name,
                total_runs,
            )
            return best_profile

        # 3) Epsilon-greedy over scores
        if random.random() < cfg.epsilon:
            # Pure exploration: random profile
            chosen = random.choice(self._profiles)
            self._logger.info(
                "StrategyManager: epsilon exploration (ε=%.3f), random profile='%s'.",
                cfg.epsilon,
                chosen.name,
            )
            return chosen

        # Exploitation: pick best by score
        best_profile = self._best_profile_by_score()
        best_score = self._score_profile(best_profile.name)
        self._logger.info(
            "StrategyManager: exploitation, best profile='%s' (score=%.3f).",
            best_profile.name,
            best_score,
        )
        return best_profile
