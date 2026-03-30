"""
minecraft_auto_miner.forge package v0.1.0 – 2025-12-08
Forge-based F3 telemetry integration for Minecraft Auto Miner.

This package is responsible for:
- Tailing the Forge-side F3 telemetry log (mam_f3_stream.log).
- Providing a clean Python API for:
    * Pose (player x/y/z, yaw, pitch, dimension, sprinting, ground)
    * TargetBlock (block under crosshair + coordinates)

Exports:
- Pose
- TargetBlock
- MotionEstimate
- ForgePoseProvider
"""

from .pose_provider import ForgePoseProvider, MotionEstimate, Pose, TargetBlock

__all__ = ["Pose", "TargetBlock", "MotionEstimate", "ForgePoseProvider"]
