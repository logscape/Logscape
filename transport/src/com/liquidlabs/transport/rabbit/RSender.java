package com.liquidlabs.transport.rabbit;

import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class RSender extends RConnector {
    private final String queueName;
    private final Channel channel;

    public RSender(RConfig config, String QueueName) throws NoSuchAlgorithmException, KeyManagementException, URISyntaxException, IOException, TimeoutException {
            super(config);
        queueName = QueueName;
        channel = config.getChannel(queueName);
    }

    public void sendMessage(String text) throws Exception {
        System.out.println("RSending:" + text);

        channel.basicPublish("", queueName,  null, text.getBytes());
    }
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

}
