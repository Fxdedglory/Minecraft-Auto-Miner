// MamForgeTelemetry.java v0.1.0 – 2025-12-08
// Minecraft Auto Miner – Forge F3 Telemetry Mod
//
// Writes one JSON line per client tick to:
//   <gameDir>/mam_telemetry/mam_f3_stream.log
//
// This is a skeleton: adjust package, Minecraft/Forge imports,
// and mappings for your exact version.

package com.fxdedglory.mamtelemetry;

import com.google.gson.JsonObject;
import com.mojang.logging.LogUtils;
import net.minecraft.client.Minecraft;
import net.minecraft.client.player.LocalPlayer;
import net.minecraft.core.BlockPos;
import net.minecraft.core.registries.BuiltInRegistries;
import net.minecraft.resources.ResourceLocation;
import net.minecraft.world.level.Level;
import net.minecraft.world.phys.BlockHitResult;
import net.minecraft.world.phys.HitResult;
import net.minecraftforge.api.distmarker.Dist;
import net.minecraftforge.client.event.ClientTickEvent;
import net.minecraftforge.eventbus.api.SubscribeEvent;
import net.minecraftforge.fml.common.Mod;
import net.minecraftforge.fml.loading.FMLPaths;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

@Mod(MamForgeTelemetry.MOD_ID)
public class MamForgeTelemetry {

    public static final String MOD_ID = "mam_forge_telemetry";
    private static final Logger LOGGER = LogUtils.getLogger();
    private static final long MAX_LOG_BYTES = 128L * 1024L * 1024L;

    private static Path logFilePath;
    private static BufferedWriter logWriter;
    private static boolean telemetryDisabled = false;

    public MamForgeTelemetry() {
        initLogFile();
        LOGGER.info("[{}] MamForgeTelemetry initialized", MOD_ID);
    }

    private static void initLogFile() {
        try {
            Path gameDir = FMLPaths.GAMEDIR.get();
            Path dir = gameDir.resolve("mam_telemetry");
            Files.createDirectories(dir);

            logFilePath = dir.resolve("mam_f3_stream.log");
            rotateIfOversized();
            logWriter = Files.newBufferedWriter(
                    logFilePath,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND,
                    StandardOpenOption.WRITE
            );
            telemetryDisabled = false;

            LOGGER.info("[{}] Writing F3 telemetry to {}", MOD_ID, logFilePath);
        } catch (IOException e) {
            LOGGER.error("[{}] Failed to initialize telemetry log file", MOD_ID, e);
            logWriter = null;
        }
    }

    private static void closeWriterQuietly() {
        if (logWriter == null) {
            return;
        }
        try {
            logWriter.close();
        } catch (IOException ignore) {
        }
        logWriter = null;
    }

    private static void rotateIfOversized() throws IOException {
        if (logFilePath == null || !Files.exists(logFilePath)) {
            return;
        }
        long size = Files.size(logFilePath);
        if (size < MAX_LOG_BYTES) {
            return;
        }
        Path rotatedPath = logFilePath.resolveSibling(
                "mam_f3_stream-" + Instant.now().toEpochMilli() + ".log"
        );
        closeWriterQuietly();
        Files.move(logFilePath, rotatedPath, StandardCopyOption.REPLACE_EXISTING);
        LOGGER.warn(
                "[{}] Rotated oversized telemetry log to {} ({} bytes)",
                MOD_ID,
                rotatedPath,
                size
        );
    }

    private static boolean reopenWriter() {
        closeWriterQuietly();
        initLogFile();
        return logWriter != null;
    }

    private static void appendJsonLine(JsonObject obj) {
        if (telemetryDisabled) {
            return;
        }
        if (logWriter == null) {
            initLogFile();
            if (logWriter == null) {
                return;
            }
        }
        try {
            rotateIfOversized();
            if (logWriter == null && !reopenWriter()) {
                return;
            }
            logWriter.write(obj.toString());
            logWriter.write("\n");
            logWriter.flush();
        } catch (IOException e) {
            LOGGER.error("[{}] Failed to write telemetry JSON line; attempting recovery", MOD_ID, e);
            if (reopenWriter()) {
                try {
                    logWriter.write(obj.toString());
                    logWriter.write("\n");
                    logWriter.flush();
                    LOGGER.warn("[{}] Telemetry writer recovered after write failure.", MOD_ID);
                    return;
                } catch (IOException retryError) {
                    LOGGER.error(
                            "[{}] IO failed writing to {} – disabling telemetry for this session.",
                            MOD_ID,
                            logFilePath,
                            retryError
                    );
                }
            } else {
                LOGGER.error(
                        "[{}] IO failed writing to {} – disabling telemetry for this session.",
                        MOD_ID,
                        logFilePath,
                        e
                );
            }
            telemetryDisabled = true;
            closeWriterQuietly();
        }
    }

    @Mod.EventBusSubscriber(modid = MOD_ID, bus = Mod.EventBusSubscriber.Bus.FORGE, value = Dist.CLIENT)
    public static class ClientEvents {

        @SubscribeEvent
        public static void onClientTick(ClientTickEvent event) {
            if (event.phase != ClientTickEvent.Phase.END) {
                return;
            }

            Minecraft mc = Minecraft.getInstance();
            if (mc == null || mc.level == null || mc.player == null) {
                return;
            }

            LocalPlayer player = mc.player;
            Level level = mc.level;

            // Basic pose
            double x = player.getX();
            double y = player.getY();
            double z = player.getZ();
            float yaw = player.getYRot();
            float pitch = player.getXRot();

            // Dimension id
            ResourceLocation dimKey = level.dimension().location();
            String dimensionId = dimKey.toString();

            // Movement flags
            boolean isSprinting = player.isSprinting();
            boolean onGround = player.onGround();

            // Crosshair / target block
            HitResult hit = mc.hitResult;
            String targetBlockId = "minecraft:air";
            Integer targetX = null;
            Integer targetY = null;
            Integer targetZ = null;

            if (hit instanceof BlockHitResult bhr) {
                BlockPos pos = bhr.getBlockPos();
                targetX = pos.getX();
                targetY = pos.getY();
                targetZ = pos.getZ();

                var blockState = level.getBlockState(pos);
                var block = blockState.getBlock();
                ResourceLocation blockKey = BuiltInRegistries.BLOCK.getKey(block);
                targetBlockId = blockKey.toString();
            }

            // Timestamp
            String tsUtc = DateTimeFormatter.ISO_INSTANT.format(Instant.now());

            // Build JSON line
            JsonObject root = new JsonObject();
            root.addProperty("type", "FORGE_F3");
            root.addProperty("ts_utc", tsUtc);

            JsonObject pose = new JsonObject();
            pose.addProperty("x", x);
            pose.addProperty("y", y);
            pose.addProperty("z", z);
            pose.addProperty("yaw", yaw);
            pose.addProperty("pitch", pitch);
            pose.addProperty("dimension", dimensionId);
            pose.addProperty("is_sprinting", isSprinting);
            pose.addProperty("on_ground", onGround);

            JsonObject target = new JsonObject();
            target.addProperty("block_id", targetBlockId);
            if (targetX != null) {
                target.addProperty("x", targetX);
                target.addProperty("y", targetY);
                target.addProperty("z", targetZ);
            }

            root.add("pose", pose);
            root.add("target", target);

            appendJsonLine(root);
        }
    }
}
