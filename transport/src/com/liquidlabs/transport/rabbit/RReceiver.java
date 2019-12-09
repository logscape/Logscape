package com.liquidlabs.transport.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.List;

public class RReceiver extends  RConnector {

    private final Channel channel;
    private List<String> cache;
    private final String queueName;

    public RReceiver(List<String> cache, RConfig config, String queueName) throws Exception {
        super(config);
        this.cache = cache;
        this.queueName = queueName;
        this.channel = config.getChannel(queueName);
    }

    public void receive() throws Exception {
        channel.basicConsume(queueName, true, newConsumer(channel));
    }

    private DefaultConsumer newConsumer(Channel channel) {
        return new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties,
                                       byte[] body) throws IOException {
                System.out.println("RReceiver GOT:" + new String(body));
                cache.add(new String(body));  // put each message into the cache
            }
        };
    }
}
