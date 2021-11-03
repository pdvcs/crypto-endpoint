package net.pdutta.rocksdemo;

import net.pdutta.cryptotool.CryptoTool;
import net.pdutta.cryptotool.KeyOnDiskSecretProvider;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Locale;

public class LocalTaskRunner implements Runnable {

    public void run() {
        log.info("setting up local task runner");
        while (!repo.canStop()) {
            ReadOptions readOptions = new ReadOptions();
            readOptions.setTailing(true);
            if (columnFamily != null && !columnFamily.equals("")) {
                ColumnFamilyHandle cfHandle = repo.familyHandleByName(RocksDBRepository.CRYPTOJOBS_CF);
                try (RocksIterator iter = repo.getDb().newIterator(cfHandle, readOptions)) {
                    processEachKey(iter);
                }
            } else {
                try (RocksIterator iter = repo.getDb().newIterator(readOptions)) {
                    processEachKey(iter);
                }
            }
        }
    }

    public LocalTaskRunner() {
        jsonReader = new JsonReader();
        KeyOnDiskSecretProvider secretProvider = new KeyOnDiskSecretProvider();
        secretProvider.setKeyFilename(SECRET_KEY_FILE);

        tool = new CryptoTool();
        tool.setSecretProvider(secretProvider);
    }

    private void processEachKey(RocksIterator iter) {
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            String k = new String(iter.key());
            String v = new String(iter.value());
            cryptoJob(k, v);
            repo.delete(RocksDBRepository.CRYPTOJOBS_CF, k);
            if (repo.canStop()) {
                break;
            }
        }
    }

    @SuppressWarnings("UnusedReturnValue")
    private int cryptoJob(String jobRef, String jobJson) {
        int result = -1;
        try {
            log.info("json:\n" + jobJson);
            HashMap<String, String> json = jsonReader.readString(jobJson);
            if (json.size() >= 2) {
                final String action = json.getOrDefault("action", "");
                final String inputFile = json.getOrDefault("input", "");
                final String outputFile = json.getOrDefault("output", "");
                if (isNonEmptyString(action) && isNonEmptyString(inputFile)) {
                    result = executeCryptoAction(action, inputFile, outputFile);
                    if (result != 1) {
                        log.info("job returned code: {}", result);
                    }
                } else {
                    log.warn("invalid JSON received");
                }
            } else { // we didn't get an action/inputfile
                log.warn("JSON contained insufficient data");
            }
        } catch (Throwable t) {
            log.warn("could not run cryptoJob, error: {}, cause: {}", t.getMessage(), t.getCause());
            log.warn("job ref {}, json = {}", jobRef, jobJson);
        }
        return result;
    }

    private int executeCryptoAction(String action, String inputFile, String outputFile) {
        int result = -1;
        boolean success;
        action = action.toLowerCase(Locale.ROOT);

        try {
            switch (action) {
                case "encrypt":
                    if (isNonEmptyString(outputFile)) {
                        success = tool.encrypt(inputFile, outputFile);
                        if (success) {
                            result = 1;
                        }
                    } else {
                        log.warn("action=encrypt but no outputFile specified");
                    }
                    break;
                case "decrypt":
                    if (isNonEmptyString(outputFile)) {
                        success = tool.decrypt(inputFile, outputFile);
                        if (success) {
                            result = 1;
                        }
                    } else {
                        log.warn("action=decrypt but no outputFile specified");
                    }
                    break;
                case "checksum":
                    final String checksum = tool.checksum(inputFile);
                    log.info("checksum for {}: {}", inputFile, checksum);
                    result = 1;
                    break;
            }
        } catch (FileNotFoundException e) {
            log.warn("warning: file not found: {}", e.getMessage(), e.getCause());
            result = -10;
        }
        return result;
    }

    private boolean isNonEmptyString(String s) {
        return s != null && !s.equals("");
    }

    @SuppressWarnings("SameParameterValue")
    private static String tempDir(String fname) {
        String t = System.getProperty("java.io.tmpdir");
        if (fname == null) {
            return t;
        } else {
            return t + System.getProperty("file.separator") + fname;
        }
    }

    //<editor-fold desc="getters and setters">

    @SuppressWarnings("unused")
    public RocksDBRepository getRepo() {
        return repo;
    }

    public void setRepo(RocksDBRepository repo) {
        this.repo = repo;
    }

    @SuppressWarnings("unused")
    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    //</editor-fold>

    private RocksDBRepository repo;
    private String columnFamily;
    private final CryptoTool tool;
    private final JsonReader jsonReader;
    private static final String SECRET_KEY_FILE = tempDir("rocksdemo_sec.key");
    Logger log = LoggerFactory.getLogger(LocalTaskRunner.class);

}
