package org.loutreeee.dataMine;

import org.bukkit.GameMode;
import org.bukkit.Material;
import org.bukkit.configuration.ConfigurationSection;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.io.File;
import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public final class DataMine extends JavaPlugin implements Listener {

    // Points config (ores -> points)
    private final Map<Material, Integer> blockPoints = new HashMap<>();

    // Lifetime totals in memory (uuid -> material -> count)
    private final Map<UUID, Map<Material, Long>> playerBlocksTotal = new HashMap<>();

    // Bucket deltas in memory (uuid -> material -> deltaCount for current bucket)
    private Map<UUID, Map<Material, Integer>> bucketDeltas = new HashMap<>();

    // SQLite
    private Connection connection;

    // Schedulers
    private int bucketTaskId = -1;
    private int totalsTaskId = -1;

    // Bucket settings
    private static final int BUCKET_SECONDS = 5;

    @Override
    public void onEnable() {
        saveDefaultConfig();
        loadBlockPoints();

        try {
            initDatabase();
            createTables();
            loadTotalsFromDb(); // hydrate memory totals (optional but useful)
        } catch (SQLException e) {
            getLogger().severe("DB init failed: " + e.getMessage());
            // Disable plugin if DB is required
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
        // Stop tasks
        if (bucketTaskId != -1) getServer().getScheduler().cancelTask(bucketTaskId);
        if (totalsTaskId != -1) getServer().getScheduler().cancelTask(totalsTaskId);

        // Flush one last time (sync snapshot then async write is risky during disable, so do sync DB write)
        try {
            flushBucketToDbSync(swapBucketDeltas());
            flushTotalsToDbSync(playerBlocksTotal);
        } catch (SQLException e) {
            getLogger().severe("Final DB flush failed: " + e.getMessage());
        }

        closeDatabaseQuietly();
        getLogger().info("DataMine disabled.");
    }

    // ----------------------------
    // Config (points)
    // ----------------------------
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
            blockPoints.put(material, value);
        }

        getLogger().info("Chargé " + blockPoints.size() + " block points.");
    }

    // ----------------------------
    // Event: BlockBreak -> update memory
    // ----------------------------
    @EventHandler
    public void onBlockBreak(BlockBreakEvent event) {
        if (event.isCancelled()) return;
        if (event.getPlayer().getGameMode() != GameMode.SURVIVAL) return;

        Material material = event.getBlock().getType();
        if (!blockPoints.containsKey(material)) return; // only ores we score

        UUID uuid = event.getPlayer().getUniqueId();

        // 1) update lifetime totals in memory
        Map<Material, Long> totals = playerBlocksTotal.computeIfAbsent(uuid, k -> new HashMap<>());
        totals.put(material, totals.getOrDefault(material, 0L) + 1L);

        // 2) update bucket delta in memory
        Map<Material, Integer> deltas = bucketDeltas.computeIfAbsent(uuid, k -> new HashMap<>());
        deltas.put(material, deltas.getOrDefault(material, 0) + 1);
    }

    // ----------------------------
    // Bucket flush task (every 5s)
    // ----------------------------
    private void startBucketFlushTask() {
        long periodTicks = 20L * BUCKET_SECONDS;

        bucketTaskId = getServer().getScheduler().runTaskTimer(this, () -> {
            long bucketStart = currentBucketStartEpoch();

            // Swap bucket map instantly on main thread
            Map<UUID, Map<Material, Integer>> toFlush = swapBucketDeltas();
            if (toFlush.isEmpty()) return;

            // Flush async to avoid lag
            getServer().getScheduler().runTaskAsynchronously(this, () -> {
                try {
                    flushBucketToDb(bucketStart, toFlush);
                } catch (SQLException e) {
                    getLogger().severe("Bucket flush failed: " + e.getMessage());
                }
            });

        }, periodTicks, periodTicks).getTaskId();
    }

    /**
     * Swap the current bucket map with a fresh empty one.
     * Must be called on the main thread.
     */
    private Map<UUID, Map<Material, Integer>> swapBucketDeltas() {
        Map<UUID, Map<Material, Integer>> old = bucketDeltas;
        bucketDeltas = new HashMap<>();
        return old;
    }

    private long currentBucketStartEpoch() {
        long now = Instant.now().getEpochSecond();
        return now - (now % BUCKET_SECONDS);
    }

    // ----------------------------
    // Totals flush task (every 60s)
    // ----------------------------
    private void startTotalsFlushTask() {
        long periodTicks = 20L * 60;

        totalsTaskId = getServer().getScheduler().runTaskTimer(this, () -> {
            // Snapshot totals shallowly (we’ll iterate safely because events are on main thread;
            // but async thread should not iterate over a mutable map)
            Map<UUID, Map<Material, Long>> snapshot = deepCopyTotals(playerBlocksTotal);

            getServer().getScheduler().runTaskAsynchronously(this, () -> {
                try {
                    flushTotalsToDb(snapshot);
                } catch (SQLException e) {
                    getLogger().severe("Totals flush failed: " + e.getMessage());
                }
            });

        }, periodTicks, periodTicks).getTaskId();
    }

    private Map<UUID, Map<Material, Long>> deepCopyTotals(Map<UUID, Map<Material, Long>> source) {
        Map<UUID, Map<Material, Long>> copy = new HashMap<>();
        for (Map.Entry<UUID, Map<Material, Long>> e : source.entrySet()) {
            copy.put(e.getKey(), new HashMap<>(e.getValue()));
        }
        return copy;
    }

    // ----------------------------
    // SQLite init / schema
    // ----------------------------
    private void initDatabase() throws SQLException {
        if (!getDataFolder().exists() && !getDataFolder().mkdirs()) {
            getLogger().warning("Impossible de créer le dossier plugin: " + getDataFolder());
        }

        File dbFile = new File(getDataFolder(), "datamine.db");
        String url = "jdbc:sqlite:" + dbFile.getAbsolutePath();

        // Create connection
        connection = DriverManager.getConnection(url);

        // Recommended pragmas for a plugin (performance + safety)
        try (Statement st = connection.createStatement()) {
            st.execute("PRAGMA journal_mode=WAL;");
            st.execute("PRAGMA synchronous=NORMAL;");
            st.execute("PRAGMA foreign_keys=ON;");
        }
    }

    private void createTables() throws SQLException {
        try (Statement st = connection.createStatement()) {

            st.execute("""
                CREATE TABLE IF NOT EXISTS player_block_totals (
                  uuid TEXT NOT NULL,
                  material TEXT NOT NULL,
                  count INTEGER NOT NULL,
                  PRIMARY KEY (uuid, material)
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

    private void closeDatabaseQuietly() {
        if (connection == null) return;
        try { connection.close(); } catch (SQLException ignored) {}
        connection = null;
    }

    // ----------------------------
    // Load totals from DB (optional but useful)
    // ----------------------------
    private void loadTotalsFromDb() throws SQLException {
        playerBlocksTotal.clear();

        String sql = "SELECT uuid, material, count FROM player_block_totals;";
        try (PreparedStatement ps = connection.prepareStatement(sql);
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

        getLogger().info("Loaded totals from DB: " + playerBlocksTotal.size() + " players");
    }

    // ----------------------------
    // DB flush: bucket deltas
    // ----------------------------
    private void flushBucketToDb(long bucketStart, Map<UUID, Map<Material, Integer>> deltas) throws SQLException {
        // Upsert style for SQLite: INSERT ... ON CONFLICT DO UPDATE
        String sql = """
            INSERT INTO player_block_deltas(bucket_start, uuid, material, delta)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(bucket_start, uuid, material) DO UPDATE SET
              delta = delta + excluded.delta;
        """;

        connection.setAutoCommit(false);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
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
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    // synchronous on disable (same logic but no async)
    private void flushBucketToDbSync(Map<UUID, Map<Material, Integer>> deltas) throws SQLException {
        if (deltas.isEmpty()) return;
        long bucketStart = currentBucketStartEpoch();
        flushBucketToDb(bucketStart, deltas);
    }

    // ----------------------------
    // DB flush: totals
    // ----------------------------
    private void flushTotalsToDb(Map<UUID, Map<Material, Long>> totalsSnapshot) throws SQLException {
        String sql = """
            INSERT INTO player_block_totals(uuid, material, count)
            VALUES(?, ?, ?)
            ON CONFLICT(uuid, material) DO UPDATE SET
              count = excluded.count;
        """;

        connection.setAutoCommit(false);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            for (Map.Entry<UUID, Map<Material, Long>> playerEntry : totalsSnapshot.entrySet()) {
                String uuidStr = playerEntry.getKey().toString();
                for (Map.Entry<Material, Long> matEntry : playerEntry.getValue().entrySet()) {
                    long count = matEntry.getValue();
                    if (count <= 0) continue;

                    ps.setString(1, uuidStr);
                    ps.setString(2, matEntry.getKey().name());
                    ps.setLong(3, count);
                    ps.addBatch();
                }
            }
            ps.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            connection.rollback();
            throw e;
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private void flushTotalsToDbSync(Map<UUID, Map<Material, Long>> totalsSnapshot) throws SQLException {
        flushTotalsToDb(deepCopyTotals(totalsSnapshot));
    }
}