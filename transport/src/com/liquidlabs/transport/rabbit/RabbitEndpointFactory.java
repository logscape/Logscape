package com.liquidlabs.transport.rabbit;

import com.liquidlabs.common.NetworkUtils;
import com.liquidlabs.common.UID;
import com.liquidlabs.common.net.URI;
import com.liquidlabs.transport.EndPoint;
import com.liquidlabs.transport.EndPointFactory;
import com.liquidlabs.transport.Receiver;
import com.liquidlabs.transport.netty.DefaultEndPoint;
import com.liquidlabs.transport.protocol.Type;
import com.liquidlabs.transport.proxy.RetryInvocationException;
import com.rabbitmq.client.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.TimeoutException;

/**
 * Created by neil on 29/06/16.
 */
public class RabbitEndpointFactory implements EndPointFactory {

    private static final Logger LOGGER = Logger.getLogger(RabbitEndpointFactory.class);

    private Connection connection;
    private java.util.Map<URI, Consumer> consumers = new java.util.concurrent.ConcurrentHashMap<>();
    private String broker;
    boolean started = false;

//    Consumer receiveConsumer;
    private MultiResponseConsumer multiResponseConsumer;
    private Channel channel;


    public RabbitEndpointFactory(String broker) {
        LOGGER.info("CREATED");

        this.broker = broker;
    }
    @Override
    public EndPoint getEndPoint(final URI uri1, final Receiver receiver) {

        try {

            final URI uri = cleanURI(uri1);

            if (!started) start();
            buildConsumerChannel(uri, receiver);

            return new DefaultEndPoint(uri, receiver) {

                @Override
                public void start() {
                }

                @Override
                public void stop() {
                }

                @Override
                public byte[] send(String protocol, URI endPoint1, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String listenerId, boolean allowlocalRoute) throws InterruptedException, RetryInvocationException {
                    try {

                        final URI endPoint = cleanURI(endPoint1);
                        String remoteURI = endPoint.toString().replace("//?", "/?");

                        if (isReplyExpected) {
                            //synchronized (channel) {
                                AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder().replyTo(multiResponseConsumer.getResponseChannel()).correlationId(UID.getUUID()).build();
                                if (LOGGER.isDebugEnabled()) LOGGER.debug("============ SEND:" + remoteURI);

                            channel.basicPublish("", remoteURI, basicProperties, bytes);

                                return multiResponseConsumer.getResults(remoteURI, basicProperties.getCorrelationId());
                            //}
                        } else {
                            if (LOGGER.isDebugEnabled()) LOGGER.debug("============ SEND:" + remoteURI);
                            channel.basicPublish("", remoteURI, null, bytes);
                        }


                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    return new byte[0];
                }

                @Override
                public byte[] receive(byte[] payload, String remoteAddress, String remoteHostname) throws InterruptedException {
                    return new byte[0];
                }

                @Override
                public byte[] receive(Object payload, String remoteAddress, String remoteHostname) {
                    return new byte[0];
                }
            };
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void buildConsumerChannel(final URI uri, final Receiver receiver) {
//        if (receiveConsumer == null) {
        Consumer consumer = consumers.get(uri);
        if (consumer == null) {

        try {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("============ CREATED QQQQQQQQQQ:\n\t" + uri.toString() + " Recv: " + receiver);

                channel.queueDeclare(uri.toString(), false, false, false, null);

                multiResponseConsumer = new MultiResponseConsumer(channel);

                consumer = new DefaultConsumer(channel) {
                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        try {
                            if (LOGGER.isDebugEnabled()) LOGGER.debug("============ INCOMING QQQQQQQQQQ:\n\t" + uri.toString());
                            byte[] response = receiver.receive(body, "unknown", "unknown");
                            if (response != null && response.length > 0) {
                                AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder().correlationId(properties.getCorrelationId());
                                if (LOGGER.isDebugEnabled()) LOGGER.debug("============  SVR REPLYING:" + uri.toString() + "\n\t Reply:" + properties.getReplyTo() + ":" + properties.getCorrelationId() + " bytes[]" + response.length);
                                channel.basicPublish("", properties.getReplyTo(), builder.build(), response);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };

                channel.basicConsume(uri.toString(), true, consumer);
                consumers.put(uri, consumer);
            } catch (IOException e1) {
                e1.printStackTrace();
                throw new RuntimeException("Cannot create: " + uri, e1);
            }
        }  else {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("============ WARN ALREADY CREATED QQQQQQQQQQ:\n\t" + uri.toString() + " Recv: " + receiver);
        }
    }

    @Override
    public void start() {
        started = true;
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(broker);
        try {
            factory.setUsername(System.getProperty("rabbit.user", "guest"));
            factory.setPassword(System.getProperty("rabbit.pwd", "guest"));
            connection = factory.newConnection();
            channel = connection.createChannel();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() {
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private URI cleanURI(URI uri) {
        try {
            String host = uri.getHost();
            if (host.equals("localhost")) host = NetworkUtils.getDefaultIpFromRoutingTable(NetworkUtils.getIPAddress());
            return new URI(uri.getScheme() + "://" + host + ":" + uri.getPort());
        } catch (URISyntaxException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
