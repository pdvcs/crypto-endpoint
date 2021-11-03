package net.pdutta.rocksdemo;

import io.muserver.MuServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App implements IEndpointStopper {
    public static void main (String[] args) {
        log.info("starting application");
        Thread shutdownHook = new Thread(() -> {
            if (rocks != null) {
                rocks.close();
            }
        });
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        System.out.println("Press Ctrl+C or POST to /shutdown to shut down cleanly.");
        new App().run();
    }

    public void run() {
        rocks = new RocksDBRepository();
        rocks.initialize();

        LocalTaskRunner localTaskRunner = new LocalTaskRunner();
        localTaskRunner.setRepo(rocks);
        localTaskRunner.setColumnFamily(RocksDBRepository.CRYPTOJOBS_CF);
        Thread t = new Thread(localTaskRunner);
        t.start();
        LocalEndpoint endpoint = new LocalEndpoint(rocks);
        endpoint.start(this);
    }
    static RocksDBRepository rocks;

    @Override
    public void stop(MuServer muServer) {
        System.out.println("App.stop(): stopping server");
        muServer.stop();
    }

    static Logger log = LoggerFactory.getLogger(App.class);
}
