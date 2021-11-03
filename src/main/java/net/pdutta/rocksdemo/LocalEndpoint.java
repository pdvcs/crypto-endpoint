package net.pdutta.rocksdemo;

import io.muserver.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Wrapper over local-only web server.
 * We only take requests from localhost, not over the network
 */
public class LocalEndpoint {

    public LocalEndpoint(KVStore repo) {
        repoHandler = new RepoHandler(repo);
        serverBuilder = MuServerBuilder.httpServer()
                .withHttpPort(11200)
                .withInterface("127.0.0.1")
                .addShutdownHook(true)
                .addHandler(Method.GET, "/", (request, response, pathParams) -> response.write("Hello, world\n"))
                .addHandler(Method.GET, "/retrieve", repoHandler)
                .addHandler(Method.POST, "/store", repoHandler)
                .addHandler(Method.POST, "/shutdown", repoHandler);
    }

    public void start(IEndpointStopper stopper) {
        server = serverBuilder.start();
        repoHandler.addListener(stopper, server);
        log.info("started local-only server at {}", server.uri());
    }

    MuServer server = null;
    MuServerBuilder serverBuilder;
    RepoHandler repoHandler;
    Logger log = LoggerFactory.getLogger(LocalEndpoint.class);
}

class RepoHandler implements RouteHandler {
    @Override
    public void handle(MuRequest request, MuResponse response, Map<String, String> pathParams) {
        log.trace("handle(): uri = " + request.uri());
        String asciiUri = request.uri().toASCIIString();
        String ns = null, key = null, value = null;

        if (asciiUri.endsWith("/store")) {
            try {
                ns = request.form().get("ns");
                key = request.form().get("key");
                value = request.form().get("val");
            } catch (IOException e) {
                log.error("error getting form values: exception: {}, message: {}", e.getCause(), e.getMessage());
            }

        } else if (asciiUri.contains("/retrieve")) {
            ns = request.query().get("ns");
            key = request.query().get("key");

        } else if (asciiUri.endsWith("/shutdown")) {
            // clean up
            repo.close();
            response.contentType("text/plain");
            response.write("shutting down local endpoint...\n");
            // tell server to stop
            emitStop();

        }
        String output = "";

        if (key != null && value != null) {
            log.trace("trying to store: [{}]::{}={}", ns, key, value);
            boolean success = repo.save(ns, key, value);
            output = success ? "success: stored key" : "error: could not store key";
        } else if (key != null) {
            log.trace("attempting to retrieve key [{}]::{}...", ns, key);
            Optional<String> result = repo.find(ns, key);
            output = result.orElse("error: could not find key");
        }
        response.contentType("text/plain");
        response.write(output + '\n');
    }

    public RepoHandler(KVStore repo) {
        this.repo = repo;
    }

    public void addListener(IEndpointStopper stopper, MuServer server) {
        stoppers.add(stopper);
        this.server = server;
    }

    public void emitStop() {
        for (IEndpointStopper s : stoppers) {
            s.stop(server);
        }
    }

    KVStore repo;
    private MuServer server;
    private final List<IEndpointStopper> stoppers = new ArrayList<>();
    Logger log = LoggerFactory.getLogger(RepoHandler.class);

}
