package net.pdutta.cryptoendpoint;

import io.muserver.MuServer;

public interface IEndpointStopper {
    void stop(MuServer muServer);
}
