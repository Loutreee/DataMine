package org.loutreeee.dataMine;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.bukkit.GameMode;
import org.bukkit.Material;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.entity.EntityDeathEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public final class DataMine extends JavaPlugin implements Listener {

    private final Map<Material, Integer> blockPoints = new HashMap<>();
    private final Map<EntityType, Integer> mobPoints = new HashMap<>();

    private final Map<UUID, Map<Material, Long>> playerBlocksTotal = new HashMap<>();
    private final Map<UUID, Map<EntityType, Long>> playerMobsTotal = new HashMap<>();
    private final Map<UUID, Long> playerPointsTotal = new HashMap<>();

    private Map<UUID, Map<Material, Integer>> bucketDeltas = new HashMap<>();
    private Map<UUID, Map<EntityType, Integer>> mobBucketDeltas = new HashMap<>();
    private Map<UUID, Integer> pointsBucketDeltas = new HashMap<>();
    private final Queue<BucketFlushPayload> failedBucketFlushes = new ConcurrentLinkedQueue<>();
    private final Queue<MobBucketFlushPayload> failedMobBucketFlushes = new ConcurrentLinkedQueue<>();
    private final Queue<PointsBucketFlushPayload> failedPointsBucketFlushes = new ConcurrentLinkedQueue<>();

    private Connection connection;
    private final Object dbLock = new Object();
    private final AtomicInteger pendingAsyncWrites = new AtomicInteger(0);
    private volatile boolean shuttingDown = false;

    private int bucketTaskId = -1;
    private int totalsTaskId = -1;

    private static final int BUCKET_SECONDS = 5;
    private static final long ASYNC_DRAIN_TIMEOUT_MS = 5000L;
    private static final int WEB_SNAPSHOT_TIMEOUT_SECONDS = 2;

    private HttpServer webServer;
    private ExecutorService webExecutor;

    private record BucketFlushPayload(long bucketStart, Map<UUID, Map<Material, Integer>> deltas) {}
    private record MobBucketFlushPayload(long bucketStart, Map<UUID, Map<EntityType, Integer>> deltas) {}
    private record PointsBucketFlushPayload(long bucketStart, Map<UUID, Integer> deltas) {}
    private record SeriesPoint(long bucketStart, String uuid, long delta, long total) {}
    private record WebSnapshot(Map<UUID, Map<Material, Long>> blockTotals,
                               Map<UUID, Map<EntityType, Long>> mobTotals,
                               Map<UUID, Long> pointTotals,
                               List<Material> trackedMaterials,
                               List<EntityType> trackedMobs) {}

    @Override
    public void onEnable() {
        shuttingDown = false;
        saveDefaultConfig();
        loadBlockPoints();
        loadMobPoints();

        try {
            initDatabase();
            createTables();
            loadTotalsFromDb();
            loadMobTotalsFromDb();
            loadPointsTotalsFromDb();
        } catch (SQLException e) {
            getLogger().severe("DB init failed: " + e.getMessage());
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        getServer().getPluginManager().registerEvents(this, this);

        startBucketFlushTask();
        startTotalsFlushTask();
        startWebServer();

        getLogger().info("DataMine enabled. DB ready. Bucket=" + BUCKET_SECONDS + "s");
    }

    @Override
    public void onDisable() {
        shuttingDown = true;
        stopWebServer();

        if (bucketTaskId != -1) getServer().getScheduler().cancelTask(bucketTaskId);
        if (totalsTaskId != -1) getServer().getScheduler().cancelTask(totalsTaskId);
        waitForPendingAsyncWrites();

        if (connection != null) {
            try {
                flushFailedBucketsToDbSync();
                flushFailedMobBucketsToDbSync();
                flushFailedPointsBucketsToDbSync();
                flushBucketToDbSync(swapBucketDeltas());
                flushMobBucketToDbSync(swapMobBucketDeltas());
                flushPointsBucketToDbSync(swapPointsBucketDeltas());
                flushTotalsToDbSync(playerBlocksTotal, playerMobsTotal, playerPointsTotal);
            } catch (SQLException e) {
                getLogger().severe("Final DB flush failed: " + e.getMessage());
            }
        }

        closeDatabaseQuietly();
        getLogger().info("DataMine disabled.");
    }

    private void loadBlockPoints() {
        ConfigurationSection section = getConfig().getConfigurationSection("points.blocks");
        if (section == null) {
            getLogger().warning("Section points.blocks introuvable !");
            return;
        }

        blockPoints.clear();

        for (String key : section.getKeys(false)) {
            Material material = Material.matchMaterial(key);
            if (material == null) {
                getLogger().warning("Material invalide dans config: " + key);
                continue;
            }

            int value = section.getInt(key);
            if (value <= 0) {
                getLogger().warning("Valeur de points invalide (<=0) pour: " + key);
                continue;
            }

            blockPoints.put(material, value);
        }

        getLogger().info("Loaded " + blockPoints.size() + " block point rules.");
    }

    private void loadMobPoints() {
        ConfigurationSection section = getConfig().getConfigurationSection("points.mobs");
        if (section == null) {
            getLogger().warning("Section points.mobs introuvable !");
            return;
        }

        mobPoints.clear();

        for (String key : section.getKeys(false)) {
            EntityType entityType;
            try {
                entityType = EntityType.valueOf(key);
            } catch (IllegalArgumentException e) {
                getLogger().warning("Mob invalide dans config: " + key);
                continue;
            }

            int value = section.getInt(key);
            if (value <= 0) {
                getLogger().warning("Valeur de points invalide (<=0) pour mob: " + key);
                continue;
            }

            mobPoints.put(entityType, value);
        }

        getLogger().info("Loaded " + mobPoints.size() + " mob point rules.");
    }

    @EventHandler
    public void onBlockBreak(BlockBreakEvent event) {
        if (event.isCancelled()) return;
        if (event.getPlayer().getGameMode() != GameMode.SURVIVAL) return;

        Material material = event.getBlock().getType();
        Integer points = blockPoints.get(material);
        if (points == null) return;

        UUID uuid = event.getPlayer().getUniqueId();

        Map<Material, Long> totals = playerBlocksTotal.computeIfAbsent(uuid, k -> new HashMap<>());
        totals.put(material, totals.getOrDefault(material, 0L) + 1L);
        playerPointsTotal.put(uuid, playerPointsTotal.getOrDefault(uuid, 0L) + points.longValue());
        long newTotalPoints = playerPointsTotal.get(uuid);

        // debug: retour joueur instantane pour gain de points et total cumule.
        event.getPlayer().sendMessage(
                "[DataMine] +" + points + " points (" + material.name() + ") | Total: " + newTotalPoints
        );
        // debug: garder ce message pour verifier en jeu les increments de points.

        Map<Material, Integer> deltas = bucketDeltas.computeIfAbsent(uuid, k -> new HashMap<>());
        deltas.put(material, deltas.getOrDefault(material, 0) + 1);

        pointsBucketDeltas.put(uuid, pointsBucketDeltas.getOrDefault(uuid, 0) + points);
    }

    @EventHandler
    public void onEntityDeath(EntityDeathEvent event) {
        Player killer = event.getEntity().getKiller();
        if (killer == null) return;
        if (killer.getGameMode() != GameMode.SURVIVAL) return;

        EntityType entityType = event.getEntityType();
        Integer points = mobPoints.get(entityType);
        if (points == null) return;

        UUID uuid = killer.getUniqueId();

        Map<EntityType, Long> totals = playerMobsTotal.computeIfAbsent(uuid, k -> new HashMap<>());
        totals.put(entityType, totals.getOrDefault(entityType, 0L) + 1L);
        playerPointsTotal.put(uuid, playerPointsTotal.getOrDefault(uuid, 0L) + points.longValue());
        long newTotalPoints = playerPointsTotal.get(uuid);

        // debug: retour joueur instantane pour gain de points (mob kill) et total cumule.
        killer.sendMessage(
                "[DataMine] +" + points + " points (KILL " + entityType.name() + ") | Total: " + newTotalPoints
        );
        // debug: garder ce message pour verifier en jeu les increments de points sur mobs.

        Map<EntityType, Integer> deltas = mobBucketDeltas.computeIfAbsent(uuid, k -> new HashMap<>());
        deltas.put(entityType, deltas.getOrDefault(entityType, 0) + 1);

        pointsBucketDeltas.put(uuid, pointsBucketDeltas.getOrDefault(uuid, 0) + points);
    }

    private void startBucketFlushTask() {
        long periodTicks = 20L * BUCKET_SECONDS;

        bucketTaskId = getServer().getScheduler().runTaskTimer(this, () -> {
            if (shuttingDown) return;
            long bucketStart = currentBucketStartEpoch();

            Map<UUID, Map<Material, Integer>> currentBucketDeltas = swapBucketDeltas();
            List<BucketFlushPayload> payloads = drainFailedBucketFlushes();
            if (!currentBucketDeltas.isEmpty()) {
                payloads.add(new BucketFlushPayload(bucketStart, currentBucketDeltas));
            }

            for (BucketFlushPayload payload : payloads) {
                submitBucketFlushAsync(payload);
            }

            Map<UUID, Map<EntityType, Integer>> currentMobBucketDeltas = swapMobBucketDeltas();
            List<MobBucketFlushPayload> mobPayloads = drainFailedMobBucketFlushes();
            if (!currentMobBucketDeltas.isEmpty()) {
                mobPayloads.add(new MobBucketFlushPayload(bucketStart, currentMobBucketDeltas));
            }

            for (MobBucketFlushPayload payload : mobPayloads) {
                submitMobBucketFlushAsync(payload);
            }

            Map<UUID, Integer> currentPointsBucketDeltas = swapPointsBucketDeltas();
            List<PointsBucketFlushPayload> pointsPayloads = drainFailedPointsBucketFlushes();
            if (!currentPointsBucketDeltas.isEmpty()) {
                pointsPayloads.add(new PointsBucketFlushPayload(bucketStart, currentPointsBucketDeltas));
            }

            for (PointsBucketFlushPayload payload : pointsPayloads) {
                submitPointsBucketFlushAsync(payload);
            }

        }, periodTicks, periodTicks).getTaskId();
    }

    private Map<UUID, Map<Material, Integer>> swapBucketDeltas() {
        Map<UUID, Map<Material, Integer>> old = bucketDeltas;
        bucketDeltas = new HashMap<>();
        return old;
    }

    private Map<UUID, Map<EntityType, Integer>> swapMobBucketDeltas() {
        Map<UUID, Map<EntityType, Integer>> old = mobBucketDeltas;
        mobBucketDeltas = new HashMap<>();
        return old;
    }

    private Map<UUID, Integer> swapPointsBucketDeltas() {
        Map<UUID, Integer> old = pointsBucketDeltas;
        pointsBucketDeltas = new HashMap<>();
        return old;
    }

    private void submitBucketFlushAsync(BucketFlushPayload payload) {
        pendingAsyncWrites.incrementAndGet();
        try {
            getServer().getScheduler().runTaskAsynchronously(this, () -> {
                try {
                    flushBucketToDb(payload.bucketStart(), payload.deltas());
                } catch (SQLException e) {
                    failedBucketFlushes.offer(payload);
                    getLogger().severe("Bucket flush failed for bucket " + payload.bucketStart() + ": " + e.getMessage());
                } finally {
                    pendingAsyncWrites.decrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            failedBucketFlushes.offer(payload);
            pendingAsyncWrites.decrementAndGet();
            getLogger().severe("Unable to schedule bucket flush: " + e.getMessage());
        }
    }

    private void submitMobBucketFlushAsync(MobBucketFlushPayload payload) {
        pendingAsyncWrites.incrementAndGet();
        try {
            getServer().getScheduler().runTaskAsynchronously(this, () -> {
                try {
                    flushMobBucketToDb(payload.bucketStart(), payload.deltas());
                } catch (SQLException e) {
                    failedMobBucketFlushes.offer(payload);
                    getLogger().severe("Mob bucket flush failed for bucket " + payload.bucketStart() + ": " + e.getMessage());
                } finally {
                    pendingAsyncWrites.decrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            failedMobBucketFlushes.offer(payload);
            pendingAsyncWrites.decrementAndGet();
            getLogger().severe("Unable to schedule mob bucket flush: " + e.getMessage());
        }
    }

    private void submitPointsBucketFlushAsync(PointsBucketFlushPayload payload) {
        pendingAsyncWrites.incrementAndGet();
        try {
            getServer().getScheduler().runTaskAsynchronously(this, () -> {
                try {
                    flushPointsBucketToDb(payload.bucketStart(), payload.deltas());
                } catch (SQLException e) {
                    failedPointsBucketFlushes.offer(payload);
                    getLogger().severe("Points bucket flush failed for bucket " + payload.bucketStart() + ": " + e.getMessage());
                } finally {
                    pendingAsyncWrites.decrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            failedPointsBucketFlushes.offer(payload);
            pendingAsyncWrites.decrementAndGet();
            getLogger().severe("Unable to schedule points bucket flush: " + e.getMessage());
        }
    }

    private long currentBucketStartEpoch() {
        long now = Instant.now().getEpochSecond();
        return now - (now % BUCKET_SECONDS);
    }

    private void startTotalsFlushTask() {
        long periodTicks = 20L * 60;

        totalsTaskId = getServer().getScheduler().runTaskTimer(this, () -> {
            if (shuttingDown) return;

            Map<UUID, Map<Material, Long>> blockSnapshot = deepCopyTotals(playerBlocksTotal);
            Map<UUID, Map<EntityType, Long>> mobSnapshot = deepCopyMobTotals(playerMobsTotal);
            Map<UUID, Long> pointsSnapshot = new HashMap<>(playerPointsTotal);
            submitTotalsFlushAsync(blockSnapshot, mobSnapshot, pointsSnapshot);

        }, periodTicks, periodTicks).getTaskId();
    }

    private void submitTotalsFlushAsync(Map<UUID, Map<Material, Long>> blockSnapshot,
                                        Map<UUID, Map<EntityType, Long>> mobSnapshot,
                                        Map<UUID, Long> pointsSnapshot) {
        pendingAsyncWrites.incrementAndGet();
        try {
            getServer().getScheduler().runTaskAsynchronously(this, () -> {
                try {
                    flushTotalsToDb(blockSnapshot, mobSnapshot, pointsSnapshot);
                } catch (SQLException e) {
                    getLogger().severe("Totals flush failed: " + e.getMessage());
                } finally {
                    pendingAsyncWrites.decrementAndGet();
                }
            });
        } catch (RuntimeException e) {
            pendingAsyncWrites.decrementAndGet();
            getLogger().severe("Unable to schedule totals flush: " + e.getMessage());
        }
    }

    private Map<UUID, Map<Material, Long>> deepCopyTotals(Map<UUID, Map<Material, Long>> source) {
        Map<UUID, Map<Material, Long>> copy = new HashMap<>();
        for (Map.Entry<UUID, Map<Material, Long>> e : source.entrySet()) {
            copy.put(e.getKey(), new HashMap<>(e.getValue()));
        }
        return copy;
    }

    private Map<UUID, Map<EntityType, Long>> deepCopyMobTotals(Map<UUID, Map<EntityType, Long>> source) {
        Map<UUID, Map<EntityType, Long>> copy = new HashMap<>();
        for (Map.Entry<UUID, Map<EntityType, Long>> e : source.entrySet()) {
            copy.put(e.getKey(), new HashMap<>(e.getValue()));
        }
        return copy;
    }

    private void initDatabase() throws SQLException {
        if (!getDataFolder().exists() && !getDataFolder().mkdirs()) {
            getLogger().warning("Impossible de créer le dossier plugin: " + getDataFolder());
        }

        File dbFile = new File(getDataFolder(), "datamine.db");
        String url = "jdbc:sqlite:" + dbFile.getAbsolutePath();

        synchronized (dbLock) {
            connection = DriverManager.getConnection(url);
            try (Statement st = connection.createStatement()) {
                st.execute("PRAGMA journal_mode=WAL;");
                st.execute("PRAGMA synchronous=NORMAL;");
                st.execute("PRAGMA foreign_keys=ON;");
            }
        }
    }

    private void createTables() throws SQLException {
        synchronized (dbLock) {
            Connection conn = requireConnection();
            try (Statement st = conn.createStatement()) {

                st.execute("""
                    CREATE TABLE IF NOT EXISTS player_block_totals (
                      uuid TEXT NOT NULL,
                      material TEXT NOT NULL,
                      count INTEGER NOT NULL,
                      PRIMARY KEY (uuid, material)
                    );
                """);

                st.execute("""
                    CREATE TABLE IF NOT EXISTS player_points_totals (
                      uuid TEXT NOT NULL PRIMARY KEY,
                      points INTEGER NOT NULL
                    );
                """);

                st.execute("""
                    CREATE TABLE IF NOT EXISTS player_mob_totals (
                      uuid TEXT NOT NULL,
                      entity_type TEXT NOT NULL,
                      count INTEGER NOT NULL,
                      PRIMARY KEY (uuid, entity_type)
                    );
                """);

                st.execute("""
                    CREATE TABLE IF NOT EXISTS player_block_deltas (
                      bucket_start INTEGER NOT NULL,
                      uuid TEXT NOT NULL,
                      material TEXT NOT NULL,
                      delta INTEGER NOT NULL,
                      PRIMARY KEY (bucket_start, uuid, material)
                    );
                """);

                st.execute("""
                    CREATE TABLE IF NOT EXISTS player_mob_deltas (
                      bucket_start INTEGER NOT NULL,
                      uuid TEXT NOT NULL,
                      entity_type TEXT NOT NULL,
                      delta INTEGER NOT NULL,
                      PRIMARY KEY (bucket_start, uuid, entity_type)
                    );
                """);

                st.execute("""
                    CREATE TABLE IF NOT EXISTS player_points_deltas (
                      bucket_start INTEGER NOT NULL,
                      uuid TEXT NOT NULL,
                      delta INTEGER NOT NULL,
                      PRIMARY KEY (bucket_start, uuid)
                    );
                """);

                st.execute("CREATE INDEX IF NOT EXISTS idx_deltas_uuid_bucket ON player_block_deltas (uuid, bucket_start);");
                st.execute("CREATE INDEX IF NOT EXISTS idx_deltas_material_bucket ON player_block_deltas (material, bucket_start);");
                st.execute("CREATE INDEX IF NOT EXISTS idx_mob_deltas_uuid_bucket ON player_mob_deltas (uuid, bucket_start);");
                st.execute("CREATE INDEX IF NOT EXISTS idx_mob_deltas_type_bucket ON player_mob_deltas (entity_type, bucket_start);");
                st.execute("CREATE INDEX IF NOT EXISTS idx_points_deltas_uuid_bucket ON player_points_deltas (uuid, bucket_start);");
            }
        }
    }

    private void closeDatabaseQuietly() {
        synchronized (dbLock) {
            if (connection == null) return;
            try {
                connection.close();
            } catch (SQLException ignored) {
            }
            connection = null;
        }
    }

    private void loadTotalsFromDb() throws SQLException {
        playerBlocksTotal.clear();

        String sql = "SELECT uuid, material, count FROM player_block_totals;";
        synchronized (dbLock) {
            Connection conn = requireConnection();
            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    UUID uuid;
                    try {
                        uuid = UUID.fromString(rs.getString("uuid"));
                    } catch (IllegalArgumentException e) {
                        continue;
                    }

                    Material mat = Material.matchMaterial(rs.getString("material"));
                    if (mat == null) continue;

                    long count = rs.getLong("count");
                    if (count <= 0) continue;

                    playerBlocksTotal
                            .computeIfAbsent(uuid, k -> new HashMap<>())
                            .put(mat, count);
                }
            }
        }

        getLogger().info("Loaded block totals from DB: " + playerBlocksTotal.size() + " players");
    }

    private void loadMobTotalsFromDb() throws SQLException {
        playerMobsTotal.clear();

        String sql = "SELECT uuid, entity_type, count FROM player_mob_totals;";
        synchronized (dbLock) {
            Connection conn = requireConnection();
            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    UUID uuid;
                    try {
                        uuid = UUID.fromString(rs.getString("uuid"));
                    } catch (IllegalArgumentException e) {
                        continue;
                    }

                    EntityType entityType;
                    try {
                        entityType = EntityType.valueOf(rs.getString("entity_type"));
                    } catch (IllegalArgumentException e) {
                        continue;
                    }

                    long count = rs.getLong("count");
                    if (count <= 0) continue;

                    playerMobsTotal
                            .computeIfAbsent(uuid, k -> new HashMap<>())
                            .put(entityType, count);
                }
            }
        }

        getLogger().info("Loaded mob totals from DB: " + playerMobsTotal.size() + " players");
    }

    private void loadPointsTotalsFromDb() throws SQLException {
        playerPointsTotal.clear();

        String sql = "SELECT uuid, points FROM player_points_totals;";
        synchronized (dbLock) {
            Connection conn = requireConnection();
            try (PreparedStatement ps = conn.prepareStatement(sql);
                 ResultSet rs = ps.executeQuery()) {

                while (rs.next()) {
                    UUID uuid;
                    try {
                        uuid = UUID.fromString(rs.getString("uuid"));
                    } catch (IllegalArgumentException e) {
                        continue;
                    }

                    long points = rs.getLong("points");
                    if (points <= 0) continue;
                    playerPointsTotal.put(uuid, points);
                }
            }
        }

        getLogger().info("Loaded point totals from DB: " + playerPointsTotal.size() + " players");
    }

    private void flushBucketToDb(long bucketStart, Map<UUID, Map<Material, Integer>> deltas) throws SQLException {
        String sql = """
            INSERT INTO player_block_deltas(bucket_start, uuid, material, delta)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(bucket_start, uuid, material) DO UPDATE SET
              delta = delta + excluded.delta;
        """;

        synchronized (dbLock) {
            Connection conn = requireConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (Map.Entry<UUID, Map<Material, Integer>> playerEntry : deltas.entrySet()) {
                    String uuidStr = playerEntry.getKey().toString();
                    for (Map.Entry<Material, Integer> matEntry : playerEntry.getValue().entrySet()) {
                        int delta = matEntry.getValue();
                        if (delta <= 0) continue;

                        ps.setLong(1, bucketStart);
                        ps.setString(2, uuidStr);
                        ps.setString(3, matEntry.getKey().name());
                        ps.setInt(4, delta);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackError) {
                    e.addSuppressed(rollbackError);
                }
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private void flushBucketToDbSync(Map<UUID, Map<Material, Integer>> deltas) throws SQLException {
        if (deltas.isEmpty()) return;
        long bucketStart = currentBucketStartEpoch();
        flushBucketToDb(bucketStart, deltas);
    }

    private void flushMobBucketToDb(long bucketStart, Map<UUID, Map<EntityType, Integer>> deltas) throws SQLException {
        String sql = """
            INSERT INTO player_mob_deltas(bucket_start, uuid, entity_type, delta)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(bucket_start, uuid, entity_type) DO UPDATE SET
              delta = delta + excluded.delta;
        """;

        synchronized (dbLock) {
            Connection conn = requireConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (Map.Entry<UUID, Map<EntityType, Integer>> playerEntry : deltas.entrySet()) {
                    String uuidStr = playerEntry.getKey().toString();
                    for (Map.Entry<EntityType, Integer> mobEntry : playerEntry.getValue().entrySet()) {
                        int delta = mobEntry.getValue();
                        if (delta <= 0) continue;

                        ps.setLong(1, bucketStart);
                        ps.setString(2, uuidStr);
                        ps.setString(3, mobEntry.getKey().name());
                        ps.setInt(4, delta);
                        ps.addBatch();
                    }
                }
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackError) {
                    e.addSuppressed(rollbackError);
                }
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private void flushMobBucketToDbSync(Map<UUID, Map<EntityType, Integer>> deltas) throws SQLException {
        if (deltas.isEmpty()) return;
        long bucketStart = currentBucketStartEpoch();
        flushMobBucketToDb(bucketStart, deltas);
    }

    private void flushPointsBucketToDb(long bucketStart, Map<UUID, Integer> deltas) throws SQLException {
        String sql = """
            INSERT INTO player_points_deltas(bucket_start, uuid, delta)
            VALUES(?, ?, ?)
            ON CONFLICT(bucket_start, uuid) DO UPDATE SET
              delta = delta + excluded.delta;
        """;

        synchronized (dbLock) {
            Connection conn = requireConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (Map.Entry<UUID, Integer> playerEntry : deltas.entrySet()) {
                    int delta = playerEntry.getValue();
                    if (delta <= 0) continue;

                    ps.setLong(1, bucketStart);
                    ps.setString(2, playerEntry.getKey().toString());
                    ps.setInt(3, delta);
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackError) {
                    e.addSuppressed(rollbackError);
                }
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private void flushPointsBucketToDbSync(Map<UUID, Integer> deltas) throws SQLException {
        if (deltas.isEmpty()) return;
        long bucketStart = currentBucketStartEpoch();
        flushPointsBucketToDb(bucketStart, deltas);
    }

    private void flushFailedBucketsToDbSync() throws SQLException {
        for (BucketFlushPayload payload : drainFailedBucketFlushes()) {
            if (!payload.deltas().isEmpty()) {
                flushBucketToDb(payload.bucketStart(), payload.deltas());
            }
        }
    }

    private void flushFailedMobBucketsToDbSync() throws SQLException {
        for (MobBucketFlushPayload payload : drainFailedMobBucketFlushes()) {
            if (!payload.deltas().isEmpty()) {
                flushMobBucketToDb(payload.bucketStart(), payload.deltas());
            }
        }
    }

    private void flushFailedPointsBucketsToDbSync() throws SQLException {
        for (PointsBucketFlushPayload payload : drainFailedPointsBucketFlushes()) {
            if (!payload.deltas().isEmpty()) {
                flushPointsBucketToDb(payload.bucketStart(), payload.deltas());
            }
        }
    }

    private void flushTotalsToDb(Map<UUID, Map<Material, Long>> blockTotalsSnapshot,
                                 Map<UUID, Map<EntityType, Long>> mobTotalsSnapshot,
                                 Map<UUID, Long> pointsSnapshot) throws SQLException {
        String blocksSql = """
            INSERT INTO player_block_totals(uuid, material, count)
            VALUES(?, ?, ?)
            ON CONFLICT(uuid, material) DO UPDATE SET
              count = excluded.count;
        """;

        String mobsSql = """
            INSERT INTO player_mob_totals(uuid, entity_type, count)
            VALUES(?, ?, ?)
            ON CONFLICT(uuid, entity_type) DO UPDATE SET
              count = excluded.count;
        """;

        String pointsSql = """
            INSERT INTO player_points_totals(uuid, points)
            VALUES(?, ?)
            ON CONFLICT(uuid) DO UPDATE SET
              points = excluded.points;
        """;

        synchronized (dbLock) {
            Connection conn = requireConnection();
            conn.setAutoCommit(false);
            try (PreparedStatement blocksPs = conn.prepareStatement(blocksSql);
                 PreparedStatement mobsPs = conn.prepareStatement(mobsSql);
                 PreparedStatement pointsPs = conn.prepareStatement(pointsSql)) {

                for (Map.Entry<UUID, Map<Material, Long>> playerEntry : blockTotalsSnapshot.entrySet()) {
                    String uuidStr = playerEntry.getKey().toString();
                    for (Map.Entry<Material, Long> matEntry : playerEntry.getValue().entrySet()) {
                        long count = matEntry.getValue();
                        if (count <= 0) continue;

                        blocksPs.setString(1, uuidStr);
                        blocksPs.setString(2, matEntry.getKey().name());
                        blocksPs.setLong(3, count);
                        blocksPs.addBatch();
                    }
                }

                for (Map.Entry<UUID, Map<EntityType, Long>> playerEntry : mobTotalsSnapshot.entrySet()) {
                    String uuidStr = playerEntry.getKey().toString();
                    for (Map.Entry<EntityType, Long> mobEntry : playerEntry.getValue().entrySet()) {
                        long count = mobEntry.getValue();
                        if (count <= 0) continue;

                        mobsPs.setString(1, uuidStr);
                        mobsPs.setString(2, mobEntry.getKey().name());
                        mobsPs.setLong(3, count);
                        mobsPs.addBatch();
                    }
                }

                for (Map.Entry<UUID, Long> playerEntry : pointsSnapshot.entrySet()) {
                    long points = playerEntry.getValue();
                    if (points <= 0) continue;

                    pointsPs.setString(1, playerEntry.getKey().toString());
                    pointsPs.setLong(2, points);
                    pointsPs.addBatch();
                }

                blocksPs.executeBatch();
                mobsPs.executeBatch();
                pointsPs.executeBatch();
                conn.commit();
            } catch (SQLException e) {
                try {
                    conn.rollback();
                } catch (SQLException rollbackError) {
                    e.addSuppressed(rollbackError);
                }
                throw e;
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    private void flushTotalsToDbSync(Map<UUID, Map<Material, Long>> blockTotalsSnapshot,
                                     Map<UUID, Map<EntityType, Long>> mobTotalsSnapshot,
                                     Map<UUID, Long> pointsSnapshot) throws SQLException {
        flushTotalsToDb(
                deepCopyTotals(blockTotalsSnapshot),
                deepCopyMobTotals(mobTotalsSnapshot),
                new HashMap<>(pointsSnapshot)
        );
    }

    private Connection requireConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            throw new SQLException("Database connection is not available.");
        }
        return connection;
    }

    private List<BucketFlushPayload> drainFailedBucketFlushes() {
        List<BucketFlushPayload> drained = new ArrayList<>();
        BucketFlushPayload payload;
        while ((payload = failedBucketFlushes.poll()) != null) {
            drained.add(payload);
        }
        return drained;
    }

    private List<MobBucketFlushPayload> drainFailedMobBucketFlushes() {
        List<MobBucketFlushPayload> drained = new ArrayList<>();
        MobBucketFlushPayload payload;
        while ((payload = failedMobBucketFlushes.poll()) != null) {
            drained.add(payload);
        }
        return drained;
    }

    private List<PointsBucketFlushPayload> drainFailedPointsBucketFlushes() {
        List<PointsBucketFlushPayload> drained = new ArrayList<>();
        PointsBucketFlushPayload payload;
        while ((payload = failedPointsBucketFlushes.poll()) != null) {
            drained.add(payload);
        }
        return drained;
    }

    private void waitForPendingAsyncWrites() {
        long deadline = System.currentTimeMillis() + ASYNC_DRAIN_TIMEOUT_MS;
        while (pendingAsyncWrites.get() > 0 && System.currentTimeMillis() < deadline) {
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        if (pendingAsyncWrites.get() > 0) {
            getLogger().warning("Timed out while waiting for async DB writes to finish.");
        }
    }

    private void startWebServer() {
        if (!getConfig().getBoolean("web.enabled", true)) {
            getLogger().info("Web interface is disabled in config.");
            return;
        }

        int port = getConfig().getInt("web.port", 8080);
        if (port < 1 || port > 65535) {
            getLogger().warning("Invalid web.port value: " + port + ". Web interface disabled.");
            return;
        }

        try {
            webServer = HttpServer.create(new InetSocketAddress(port), 0);
            webServer.createContext("/", this::handleWebRoot);
            webExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r, "DataMine-Web");
                thread.setDaemon(true);
                return thread;
            });
            webServer.setExecutor(webExecutor);
            webServer.start();
            getLogger().info("Web interface started on http://0.0.0.0:" + port + "/");
        } catch (IOException e) {
            getLogger().severe("Unable to start web interface: " + e.getMessage());
        }
    }

    private void stopWebServer() {
        if (webServer != null) {
            webServer.stop(0);
            webServer = null;
        }

        if (webExecutor != null) {
            webExecutor.shutdownNow();
            webExecutor = null;
        }
    }

    private void handleWebRoot(HttpExchange exchange) throws IOException {
        if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
            respondText(exchange, 405, "Method Not Allowed");
            return;
        }

        if (shuttingDown) {
            respondText(exchange, 503, "DataMine is shutting down.");
            return;
        }

        String path = exchange.getRequestURI().getPath();
        if ("/api/timeseries".equals(path)) {
            handleTimeSeriesApi(exchange);
            return;
        }

        try {
            WebSnapshot snapshot = snapshotForWeb();
            String body = renderWebText(snapshot);
            respondText(exchange, 200, body);
        } catch (Exception e) {
            getLogger().warning("Web snapshot failed: " + e.getMessage());
            respondText(exchange, 500, "Unable to build stats snapshot.");
        }
    }

    private void handleTimeSeriesApi(HttpExchange exchange) throws IOException {
        Map<String, String> queryParams = parseQuery(exchange.getRequestURI());

        try {
            String metric = queryParams.getOrDefault("metric", "points").toLowerCase();
            String uuidFilter = normalizeUuidFilter(queryParams.get("uuid"));
            Long fromFilter = parseLongOrNull(queryParams.get("from"));
            Long toFilter = parseLongOrNull(queryParams.get("to"));

            if (fromFilter != null && toFilter != null && fromFilter > toFilter) {
                respondJson(exchange, 400, "{\"error\":\"Invalid range: from must be <= to.\"}");
                return;
            }

            List<SeriesPoint> series;
            String materialFilter = null;
            String entityTypeFilter = null;

            switch (metric) {
                case "points" -> series = querySeries("player_points_deltas", null, null, uuidFilter, fromFilter, toFilter);
                case "block" -> {
                    Material material = parseRequiredMaterial(queryParams.get("material"));
                    materialFilter = material.name();
                    series = querySeries("player_block_deltas", "material", materialFilter, uuidFilter, fromFilter, toFilter);
                }
                case "mob" -> {
                    EntityType entityType = parseRequiredEntityType(queryParams.get("entityType"));
                    entityTypeFilter = entityType.name();
                    series = querySeries("player_mob_deltas", "entity_type", entityTypeFilter, uuidFilter, fromFilter, toFilter);
                }
                default -> throw new IllegalArgumentException("metric must be one of: points, block, mob");
            }

            String body = buildTimeSeriesJson(metric, uuidFilter, materialFilter, entityTypeFilter, fromFilter, toFilter, series);
            respondJson(exchange, 200, body);
        } catch (IllegalArgumentException e) {
            respondJson(exchange, 400, "{\"error\":\"" + jsonEscape(e.getMessage()) + "\"}");
        } catch (SQLException e) {
            getLogger().warning("Time series query failed: " + e.getMessage());
            respondJson(exchange, 500, "{\"error\":\"Database query failed.\"}");
        }
    }

    private List<SeriesPoint> querySeries(String tableName,
                                          String dimensionColumn,
                                          String dimensionValue,
                                          String uuidFilter,
                                          Long fromFilter,
                                          Long toFilter) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT bucket_start, uuid, SUM(delta) AS value FROM ");
        sql.append(tableName).append(" WHERE 1=1");

        List<Object> params = new ArrayList<>();
        if (dimensionColumn != null && dimensionValue != null) {
            sql.append(" AND ").append(dimensionColumn).append(" = ?");
            params.add(dimensionValue);
        }
        if (uuidFilter != null) {
            sql.append(" AND uuid = ?");
            params.add(uuidFilter);
        }
        if (fromFilter != null) {
            sql.append(" AND bucket_start >= ?");
            params.add(fromFilter);
        }
        if (toFilter != null) {
            sql.append(" AND bucket_start <= ?");
            params.add(toFilter);
        }

        sql.append(" GROUP BY bucket_start, uuid ORDER BY bucket_start ASC, uuid ASC");

        List<SeriesPoint> points = new ArrayList<>();
        Map<String, Long> cumulativeByUuid = new HashMap<>();
        synchronized (dbLock) {
            Connection conn = requireConnection();
            try (PreparedStatement ps = conn.prepareStatement(sql.toString())) {
                for (int i = 0; i < params.size(); i++) {
                    Object param = params.get(i);
                    if (param instanceof Long longValue) {
                        ps.setLong(i + 1, longValue);
                    } else {
                        ps.setString(i + 1, String.valueOf(param));
                    }
                }

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long bucketStart = rs.getLong("bucket_start");
                        String uuid = rs.getString("uuid");
                        long delta = rs.getLong("value");

                        long total = cumulativeByUuid.getOrDefault(uuid, 0L) + delta;
                        cumulativeByUuid.put(uuid, total);

                        points.add(new SeriesPoint(bucketStart, uuid, delta, total));
                    }
                }
            }
        }

        return points;
    }

    private Material parseRequiredMaterial(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            throw new IllegalArgumentException("material is required when metric=block");
        }
        Material material = Material.matchMaterial(rawValue.trim());
        if (material == null) {
            throw new IllegalArgumentException("Unknown material: " + rawValue);
        }
        return material;
    }

    private EntityType parseRequiredEntityType(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            throw new IllegalArgumentException("entityType is required when metric=mob");
        }
        try {
            return EntityType.valueOf(rawValue.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown entityType: " + rawValue);
        }
    }

    private String normalizeUuidFilter(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(rawValue.trim()).toString();
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid uuid format: " + rawValue);
        }
    }

    private Long parseLongOrNull(String rawValue) {
        if (rawValue == null || rawValue.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(rawValue.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value: " + rawValue);
        }
    }

    private Map<String, String> parseQuery(URI uri) {
        Map<String, String> query = new LinkedHashMap<>();
        String rawQuery = uri.getRawQuery();
        if (rawQuery == null || rawQuery.isBlank()) {
            return query;
        }

        String[] pairs = rawQuery.split("&");
        for (String pair : pairs) {
            if (pair.isBlank()) continue;
            String[] parts = pair.split("=", 2);
            String key = URLDecoder.decode(parts[0], StandardCharsets.UTF_8);
            String value = parts.length > 1 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "";
            query.put(key, value);
        }
        return query;
    }

    private String buildTimeSeriesJson(String metric,
                                       String uuidFilter,
                                       String materialFilter,
                                       String entityTypeFilter,
                                       Long fromFilter,
                                       Long toFilter,
                                       List<SeriesPoint> series) {
        StringBuilder builder = new StringBuilder();
        builder.append("{\"metric\":\"").append(jsonEscape(metric)).append("\",");
        builder.append("\"bucketSeconds\":").append(BUCKET_SECONDS).append(",");
        builder.append("\"filters\":{");

        boolean firstFilter = true;
        firstFilter = appendJsonStringFilter(builder, firstFilter, "uuid", uuidFilter);
        firstFilter = appendJsonStringFilter(builder, firstFilter, "material", materialFilter);
        firstFilter = appendJsonStringFilter(builder, firstFilter, "entityType", entityTypeFilter);
        firstFilter = appendJsonLongFilter(builder, firstFilter, "from", fromFilter);
        appendJsonLongFilter(builder, firstFilter, "to", toFilter);
        builder.append("},");

        builder.append("\"series\":[");
        for (int i = 0; i < series.size(); i++) {
            SeriesPoint point = series.get(i);
            if (i > 0) builder.append(",");
            builder.append("{\"bucketStart\":").append(point.bucketStart())
                    .append(",\"uuid\":\"").append(jsonEscape(point.uuid())).append("\"")
                    .append(",\"delta\":").append(point.delta())
                    .append(",\"total\":").append(point.total())
                    .append("}");
        }
        builder.append("]}");
        return builder.toString();
    }

    private boolean appendJsonStringFilter(StringBuilder builder, boolean first, String key, String value) {
        if (value == null) return first;
        if (!first) builder.append(",");
        builder.append("\"").append(jsonEscape(key)).append("\":\"").append(jsonEscape(value)).append("\"");
        return false;
    }

    private boolean appendJsonLongFilter(StringBuilder builder, boolean first, String key, Long value) {
        if (value == null) return first;
        if (!first) builder.append(",");
        builder.append("\"").append(jsonEscape(key)).append("\":").append(value);
        return false;
    }

    private String jsonEscape(String raw) {
        return raw.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private WebSnapshot snapshotForWeb() throws Exception {
        Future<WebSnapshot> future = getServer().getScheduler().callSyncMethod(this, () -> {
            Map<UUID, Map<Material, Long>> blockSnapshot = deepCopyTotals(playerBlocksTotal);
            Map<UUID, Map<EntityType, Long>> mobSnapshot = deepCopyMobTotals(playerMobsTotal);
            Map<UUID, Long> pointsSnapshot = new HashMap<>(playerPointsTotal);
            List<Material> trackedMaterials = new ArrayList<>(blockPoints.keySet());
            trackedMaterials.sort(Comparator.comparing(Material::name));
            List<EntityType> trackedMobs = new ArrayList<>(mobPoints.keySet());
            trackedMobs.sort(Comparator.comparing(EntityType::name));
            return new WebSnapshot(blockSnapshot, mobSnapshot, pointsSnapshot, trackedMaterials, trackedMobs);
        });
        return future.get(WEB_SNAPSHOT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private String renderWebText(WebSnapshot snapshot) {
        StringBuilder builder = new StringBuilder();
        builder.append("DataMine Stats\n");
        builder.append("Generated: ").append(Instant.now()).append('\n');
        builder.append("API points: /api/timeseries?metric=points\n");
        builder.append("API block: /api/timeseries?metric=block&material=DIAMOND_ORE\n");
        builder.append("API mob: /api/timeseries?metric=mob&entityType=ZOMBIE\n");
        builder.append('\n');

        Set<UUID> allPlayers = new HashSet<>(snapshot.pointTotals().keySet());
        allPlayers.addAll(snapshot.blockTotals().keySet());
        allPlayers.addAll(snapshot.mobTotals().keySet());
        if (allPlayers.isEmpty()) {
            builder.append("No player data yet.\n");
            return builder.toString();
        }

        List<UUID> orderedPlayers = new ArrayList<>(allPlayers);
        orderedPlayers.sort((left, right) -> {
            long leftPoints = snapshot.pointTotals().getOrDefault(left, 0L);
            long rightPoints = snapshot.pointTotals().getOrDefault(right, 0L);
            int cmp = Long.compare(rightPoints, leftPoints);
            return cmp != 0 ? cmp : left.toString().compareTo(right.toString());
        });

        for (UUID uuid : orderedPlayers) {
            String playerName = resolvePlayerName(uuid);
            long totalPoints = snapshot.pointTotals().getOrDefault(uuid, 0L);
            Map<Material, Long> blocks = snapshot.blockTotals().getOrDefault(uuid, Map.of());
            Map<EntityType, Long> mobs = snapshot.mobTotals().getOrDefault(uuid, Map.of());

            builder.append("Player: ").append(playerName).append(" (").append(uuid).append(")\n");
            builder.append("Total points: ").append(totalPoints).append('\n');
            builder.append("Mined blocks:\n");

            for (Material material : snapshot.trackedMaterials()) {
                long count = blocks.getOrDefault(material, 0L);
                builder.append(" - ").append(material.name()).append(": ").append(count).append('\n');
            }

            builder.append("Killed mobs:\n");
            for (EntityType entityType : snapshot.trackedMobs()) {
                long count = mobs.getOrDefault(entityType, 0L);
                builder.append(" - ").append(entityType.name()).append(": ").append(count).append('\n');
            }

            builder.append('\n');
        }

        return builder.toString();
    }

    private String resolvePlayerName(UUID uuid) {
        String name = getServer().getOfflinePlayer(uuid).getName();
        return name != null ? name : "Unknown";
    }

    private void respondText(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, payload.length);
        try (OutputStream stream = exchange.getResponseBody()) {
            stream.write(payload);
        }
    }

    private void respondJson(HttpExchange exchange, int statusCode, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, payload.length);
        try (OutputStream stream = exchange.getResponseBody()) {
            stream.write(payload);
        }
    }
}
