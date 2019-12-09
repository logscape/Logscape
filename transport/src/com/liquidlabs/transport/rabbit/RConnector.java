package com.liquidlabs.transport.rabbit;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class RConnector {
    protected  RConfig config;

    public RConnector(RConfig config) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
        this.config = config;
        config.connect();
    }
}
