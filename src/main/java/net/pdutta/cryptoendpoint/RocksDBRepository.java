package net.pdutta.cryptoendpoint;

import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class RocksDBRepository implements KVStore {
    Logger log = LoggerFactory.getLogger(RocksDBRepository.class);
    private final static String FILE_NAME = "localtasks.db";
    private boolean canStop = false;
    File dbFile;
    public static final String CRYPTOJOBS_CF = "cryptojobs";

    RocksDB db;
    ColumnFamilyOptions columnFamilyOptions;
    List<ColumnFamilyHandle> columnFamilyHandles;

    void initialize() {
        columnFamilyOptions = new ColumnFamilyOptions().optimizeUniversalStyleCompaction();
        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnFamilyOptions),
                new ColumnFamilyDescriptor(CRYPTOJOBS_CF.getBytes(StandardCharsets.UTF_8), columnFamilyOptions)
        );
        columnFamilyHandles = new ArrayList<>();

        RocksDB.loadLibrary();
        final DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);

        dbFile = new File("/tmp/rocks", FILE_NAME);
        try {
            Files.createDirectories(dbFile.getParentFile().toPath());
            Files.createDirectories(dbFile.getAbsoluteFile().toPath());
            db = RocksDB.open(options, dbFile.getAbsolutePath(), cfDescriptors, columnFamilyHandles);
            log.info("RocksDB initialized");
        } catch (IOException | RocksDBException e) {
            log.error("Error initializing RocksDB. Exception: '{}', message: '{}'", e.getCause(), e.getMessage());
            System.exit(1);
        }
    }

    @SuppressWarnings("SameParameterValue")
    ColumnFamilyHandle familyHandleByName(String columnFmaily) {
        return familyHandleByName(columnFamilyHandles, columnFmaily);
    }

    private ColumnFamilyHandle familyHandleByName(List<ColumnFamilyHandle> columnFamilyHandles, String columnFamily) {
        byte[] name = columnFamily.getBytes(StandardCharsets.UTF_8);
        return columnFamilyHandles
                .stream()
                .filter(
                        handle -> {
                            try {
                                return Arrays.equals(handle.getName(), name);
                            } catch (Exception ex) {
                                throw new RuntimeException(ex);
                            }
                        })
                .findAny()
                .orElse(null);
    }

    synchronized void prepForShutdown() {
        if (!canStop) {
            log.info("cancelling all background jobs...");
            canStop = true;
            db.cancelAllBackgroundWork(true);
            log.info("done cancelling all background jobs");
        } else {
            log.info("already prepped for shutdown");
        }
    }

    public synchronized void close() {
        if (!canStop) {
            prepForShutdown();
            try {
                log.info("closing db...");
                db.syncWal();
                for (final ColumnFamilyHandle cfHandle : columnFamilyHandles) {
                    cfHandle.close();
                }
                columnFamilyOptions.close();
                db.closeE();
                log.info("done closing db");
            } catch (RocksDBException e) {
                log.error("error shutting down RocksDB. Exception: '{}', message: '{}'", e.getCause(), e.getMessage());
                e.printStackTrace();
            }
        } else {
            log.info("already closed");
        }
    }

    @Override
    public synchronized boolean save(String columnFamily, String key, String value) {
        log.info("saving [{}]::{}={}", columnFamily, key, value);
        try {
            if (isNullOrEmpty(columnFamily)) {
                db.put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
            } else {
                ColumnFamilyHandle cfHandle = familyHandleByName(columnFamilyHandles, columnFamily);
                db.put(cfHandle, key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
            }
        } catch (RocksDBException e) {
            log.error("error saving entry. Cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            return false;
        }
        log.info("save(): success");
        return true;
    }

    @Override
    public synchronized Optional<String> find(String columnFamily, String key) {
        String value = null;
        try {
            byte[] bytes;
            if (isNullOrEmpty(columnFamily)) {
                bytes = db.get(key.getBytes(StandardCharsets.UTF_8));
            } else {
                ColumnFamilyHandle cfHandle = familyHandleByName(columnFamilyHandles, columnFamily);
                bytes = db.get(cfHandle, key.getBytes(StandardCharsets.UTF_8));
            }
            if (bytes != null) {
                value = new String(bytes, StandardCharsets.UTF_8);
            }
        } catch (RocksDBException e) {
            log.error("error retrieving the entry with key: {}, cause: {}, message: {}",
                    key, e.getCause(), e.getMessage()
            );
        }
        log.info("finding key '{}' returns '{}'", key, value);
        return value != null ? Optional.of(value) : Optional.empty();
    }

    @Override
    public synchronized boolean delete(String columnFamily, String key) {
        log.info("deleting key '{}'", key);
        try {
            if (isNullOrEmpty(columnFamily)) {
                db.delete(key.getBytes(StandardCharsets.UTF_8));
            } else {
                ColumnFamilyHandle cfHandle = familyHandleByName(columnFamilyHandles, columnFamily);
                db.delete(cfHandle, key.getBytes());
            }
        } catch (RocksDBException e) {
            log.error("error deleting entry, cause: '{}', message: '{}'", e.getCause(), e.getMessage());
            return false;
        }
        return true;
    }

    static boolean isNullOrEmpty(String s) {
        return s == null || s.equals("");
    }

    public boolean canStop() {
        return canStop;
    }

    public RocksDB getDb() {
        return db;
    }
}