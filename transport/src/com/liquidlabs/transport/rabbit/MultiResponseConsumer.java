package com.liquidlabs.transport.rabbit;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.*;

/**
 * Created by neil on 01/07/16.
 */
public class MultiResponseConsumer extends DefaultConsumer {
    private static final Logger LOGGER = Logger.getLogger(MultiResponseConsumer.class);

    private final String responseChannel;

    public MultiResponseConsumer(Channel channel) throws IOException {
        super(channel);
        responseChannel = channel.queueDeclare().getQueue();
        channel.basicConsume(responseChannel, true, this);
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)  throws IOException {
        try {


            String correlationId = properties.getCorrelationId();
            if (LOGGER.isDebugEnabled()) LOGGER.debug("============  RECV-RESP for client:\n\t" + responseChannel + ":" + correlationId);
            valueMap.put(correlationId, body);
            Object waitObj = responseMap.get(correlationId);
            if (waitObj == null) {
                LOGGER.warn("DIDNT find correlationId:" + correlationId + " Keys:" + responseMap.keySet());
            }
            synchronized (waitObj) {
                waitObj.notifyAll();
            }
        } catch (Throwable t) {
            LOGGER.warn("handleDeliveryFailed", t);
        }
    }

    Map<String, Object> responseMap = new ConcurrentHashMap<>();
    Map<String, byte[]> valueMap = new ConcurrentHashMap<>();
    public byte[] getResults(String channel, String correlationId) {


        if (LOGGER.isDebugEnabled()) LOGGER.debug("============  WAITING ON: Remote:" + channel + "\n\t" + responseChannel + ":" + correlationId);

        Object waitObject = new Object();
        responseMap.put(correlationId, waitObject);
        try {
            synchronized (waitObject) {
                waitObject.wait(10 * 1000);
            }
            responseMap.remove(correlationId);
            byte[] remove = valueMap.remove(correlationId);
            if (remove == null) throw new RuntimeException("Didnt get Response: From:" + channel + " \n\t" + responseChannel + ":" + correlationId);
            else {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("============  GOT REPLY:\n\t" + responseChannel + ":" + correlationId);
            }
            return remove;

        } catch (Exception t) {
            LOGGER.warn("getResults", t);
        }
        throw  new RuntimeException("Failed to get Result");

    }
    public String getResponseChannel() {
        return responseChannel;
    }
}

