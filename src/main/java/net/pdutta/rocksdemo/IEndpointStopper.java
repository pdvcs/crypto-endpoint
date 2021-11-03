package net.pdutta.rocksdemo;

import io.muserver.MuServer;

public interface IEndpointStopper {
    void stop(MuServer muServer);
}
