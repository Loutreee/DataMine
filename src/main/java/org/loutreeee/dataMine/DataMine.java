package org.loutreeee.dataMine;

import org.bukkit.GameMode;
import org.bukkit.Material;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public final class DataMine extends JavaPlugin implements Listener {

    private final Map<Material, Integer> blockPoints = new HashMap<>();

    private final Map<UUID, Map<Material, Long>> playerBlocksTotal = new HashMap<>();
    private final Map<UUID, Long> playerPointsTotal = new HashMap<>();

    private Map<UUID, Map<Material, Integer>> bucketDeltas = new HashMap<>();
    private final Queue<BucketFlushPayload> failedBucketFlushes = new ConcurrentLinkedQueue<>();

    private Connection connection;
    private final Object dbLock = new Object();
    private final AtomicInteger pendingAsyncWrites = new AtomicInteger(0);
    private volatile boolean shuttingDown = false;

    private int bucketTaskId = -1;
    private int totalsTaskId = -1;

    private static final int BUCKET_SECONDS = 5;
    private static final long ASYNC_DRAIN_TIMEOUT_MS = 5000L;

    private record BucketFlushPayload(long bucketStart, Map<UUID, Map<Material, Integer>> deltas) {}

    @Override
    public void onEnable() {
        shuttingDown = false;
        saveDefaultConfig();
        loadBlockPoints();

        try {
            initDatabase();
            createTables();
            loadTotalsFromDb();
            loadPointsTotalsFromDb();
        } catch (SQLException e) {
            getLogger().severe("DB init failed: " + e.getMessage());
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        getServer().getPluginManager().registerEvents(this, this);

        startBucketFlushTask();
        startTotalsFlushTask();

        getLogger().info("DataMine enabled. DB ready. Bucket=" + BUCKET_SECONDS + "s");
    }

    @Override
    public void onDisable() {
        shuttingDown = true;

        if (bucketTaskId != -1) getServer().getScheduler().cancelTask(bucketTaskId);
        if (totalsTaskId != -1) getServer().getScheduler().cancelTask(totalsTaskId);
        waitForPendingAsyncWrites();

        if (connection != null) {
            try {
                flushFailedBucketsToDbSync();
                flushBucketToDbSync(swapBucketDeltas());
                flushTotalsToDbSync(playerBlocksTotal, playerPointsTotal);
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

        Map<Material, Integer> deltas = bucketDeltas.computeIfAbsent(uuid, k -> new HashMap<>());
        deltas.put(material, deltas.getOrDefault(material, 0) + 1);
    }

    private void startBucketFlushTask() {
        long periodTicks = 20L * BUCKET_SECONDS;

        bucketTaskId = getServer().getScheduler().runTaskTimer(this, () -> {
            if (shuttingDown) return;

            List<BucketFlushPayload> payloads = drainFailedBucketFlushes();

            Map<UUID, Map<Material, Integer>> currentBucketDeltas = swapBucketDeltas();
            if (!currentBucketDeltas.isEmpty()) {
                payloads.add(new BucketFlushPayload(currentBucketStartEpoch(), currentBucketDeltas));
            }

            for (BucketFlushPayload payload : payloads) {
                submitBucketFlushAsync(payload);
            }

        }, periodTicks, periodTicks).getTaskId();
    }

    private Map<UUID, Map<Material, Integer>> swapBucketDeltas() {
        Map<UUID, Map<Material, Integer>> old = bucketDeltas;
        bucketDeltas = new HashMap<>();
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

    private long currentBucketStartEpoch() {
        long now = Instant.now().getEpochSecond();
        return now - (now % BUCKET_SECONDS);
    }

    private void startTotalsFlushTask() {
        long periodTicks = 20L * 60;

        totalsTaskId = getServer().getScheduler().runTaskTimer(this, () -> {
            if (shuttingDown) return;

            Map<UUID, Map<Material, Long>> snapshot = deepCopyTotals(playerBlocksTotal);
            Map<UUID, Long> pointsSnapshot = new HashMap<>(playerPointsTotal);
            submitTotalsFlushAsync(snapshot, pointsSnapshot);

        }, periodTicks, periodTicks).getTaskId();
    }

    private void submitTotalsFlushAsync(Map<UUID, Map<Material, Long>> snapshot, Map<UUID, Long> pointsSnapshot) {
        pendingAsyncWrites.incrementAndGet();
        try {
            getServer().getScheduler().runTaskAsynchronously(this, () -> {
                try {
                    flushTotalsToDb(snapshot, pointsSnapshot);
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

    private void initDatabase() throws SQLException {
        if (!getDataFolder().exists() && !getDataFolder().mkdirs()) {
            getLogger().warning("Impossible de creer le dossier plugin: " + getDataFolder());
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
                    CREATE TABLE IF NOT EXISTS player_block_deltas (
                      bucket_start INTEGER NOT NULL,
                      uuid TEXT NOT NULL,
                      material TEXT NOT NULL,
                      delta INTEGER NOT NULL,
                      PRIMARY KEY (bucket_start, uuid, material)
                    );
                """);

                st.execute("CREATE INDEX IF NOT EXISTS idx_deltas_uuid_bucket ON player_block_deltas (uuid, bucket_start);");
                st.execute("CREATE INDEX IF NOT EXISTS idx_deltas_material_bucket ON player_block_deltas (material, bucket_start);");
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

    private void flushFailedBucketsToDbSync() throws SQLException {
        for (BucketFlushPayload payload : drainFailedBucketFlushes()) {
            if (!payload.deltas().isEmpty()) {
                flushBucketToDb(payload.bucketStart(), payload.deltas());
            }
        }
    }

    private void flushTotalsToDb(Map<UUID, Map<Material, Long>> totalsSnapshot, Map<UUID, Long> pointsSnapshot) throws SQLException {
        String blocksSql = """
            INSERT INTO player_block_totals(uuid, material, count)
            VALUES(?, ?, ?)
            ON CONFLICT(uuid, material) DO UPDATE SET
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
                 PreparedStatement pointsPs = conn.prepareStatement(pointsSql)) {

                for (Map.Entry<UUID, Map<Material, Long>> playerEntry : totalsSnapshot.entrySet()) {
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

                for (Map.Entry<UUID, Long> playerEntry : pointsSnapshot.entrySet()) {
                    long points = playerEntry.getValue();
                    if (points <= 0) continue;

                    pointsPs.setString(1, playerEntry.getKey().toString());
                    pointsPs.setLong(2, points);
                    pointsPs.addBatch();
                }

                blocksPs.executeBatch();
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

    private void flushTotalsToDbSync(Map<UUID, Map<Material, Long>> totalsSnapshot, Map<UUID, Long> pointsSnapshot) throws SQLException {
        flushTotalsToDb(deepCopyTotals(totalsSnapshot), new HashMap<>(pointsSnapshot));
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
}
