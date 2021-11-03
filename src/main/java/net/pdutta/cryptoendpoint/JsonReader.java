package net.pdutta.cryptoendpoint;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

public class JsonReader {

    @SuppressWarnings("unused")
    public HashMap<String, String> readSmallFile(String filename) {
        HashMap<String, String> resultMap = new HashMap<>();
        String json;
        try {
            json = Files.readString(Paths.get(filename));
            //noinspection unchecked
            resultMap = gson.fromJson(json, resultMap.getClass());
        } catch (IOException | JsonSyntaxException e) {
            log.error("error reading json: {}, cause: {}", e.getMessage(), e.getCause());
        }
        return resultMap;
    }

    public HashMap<String, String> readString(String json) {
        HashMap<String, String> resultMap = new HashMap<>();
        try {
            //noinspection unchecked
            resultMap = gson.fromJson(json, resultMap.getClass());
        } catch (JsonSyntaxException e) {
            log.error("error reading json: {}, cause: {}", e.getMessage(), e.getCause());
        }
        return resultMap;
    }

    public JsonReader() {
        GsonBuilder builder = new GsonBuilder();
        gson = builder.create();
    }

    Logger log = LoggerFactory.getLogger(JsonReader.class);
    private final Gson gson;
}
