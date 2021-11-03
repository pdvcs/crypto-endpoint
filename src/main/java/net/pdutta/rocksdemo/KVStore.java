package net.pdutta.rocksdemo;

import java.util.Optional;

public interface KVStore {
    boolean save(String namespace, String key, String value);
    Optional<String> find(String namespace, String key);
    boolean delete(String namespace, String key);
    void close();
}
