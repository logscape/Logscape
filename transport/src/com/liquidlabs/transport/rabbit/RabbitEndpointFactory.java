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

public class RabbitEndpointFactory implements EndPointFactory {

     static final Logger LOGGER = Logger.getLogger(RabbitEndpointFactory.class);

    private java.util.Map<URI, Consumer> consumers = new java.util.concurrent.ConcurrentHashMap<>();
    boolean started = false;

    private MultiResponseConsumer multiResponseConsumer;
    private RConfig rConfig;

    public RabbitEndpointFactory(String brokerUrl) {
        LOGGER.info("CREATED");
        this.rConfig = new RConfig(brokerUrl);
    }
    @Override
    public EndPoint getEndPoint(final URI uri1, final Receiver receiver) {

        try {

            final URI uri = cleanURI(uri1);

            if (!started) start();
            buildConsumerChannel(uri, receiver);

            return new DefaultEndPoint(uri, receiver) {
                private Channel channel;
                @Override
                public void start() {
                }

                @Override
                public void stop() {
                    if (channel != null) {
                        try {
                            channel.close();
                            channel = null;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }

                @Override
                public byte[] send(String protocol, URI endPoint1, byte[] bytes, Type type, boolean isReplyExpected, long timeoutSeconds, String listenerId, boolean allowlocalRoute) throws InterruptedException, RetryInvocationException {

                    try {
                        final URI endPoint = cleanURI(endPoint1);
                        String remoteAddressAsQueueName = addressAsQueueName(endPoint);

                        if (channel == null) {
                            channel = rConfig.getChannel(remoteAddressAsQueueName);
                        }

                        if (isReplyExpected) {
                            synchronized (channel) {
                                AMQP.BasicProperties basicProperties = new AMQP.BasicProperties.Builder().replyTo(multiResponseConsumer.getResponseChannel()).correlationId(UID.getUUID()).build();
                                if (LOGGER.isDebugEnabled()) LOGGER.debug("============ SEND:" + remoteAddressAsQueueName);

                                channel.basicPublish("", remoteAddressAsQueueName, basicProperties, bytes);
                                return multiResponseConsumer.getResults(remoteAddressAsQueueName, basicProperties.getCorrelationId());
                            }
                        } else {
                            if (LOGGER.isDebugEnabled()) LOGGER.debug("============ SEND:" + remoteAddressAsQueueName);
                            channel.basicPublish("", remoteAddressAsQueueName, null, bytes);
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
        final URI endPoint = cleanURI(uri);
        String addressAsQueueName = addressAsQueueName(endPoint);

        Consumer consumer = consumers.get(uri);
        if (consumer == null) {

        try {
                if (LOGGER.isDebugEnabled()) LOGGER.debug("============ RECEIVER CREATED QQQQQQQQQQ: " + addressAsQueueName + " Recv: " + receiver);

                Channel channel = rConfig.getChannel(addressAsQueueName);

                multiResponseConsumer = new MultiResponseConsumer(channel);

                consumer = new ClosableConsumer(addressAsQueueName, channel, receiver);
                channel.basicConsume(addressAsQueueName, true, consumer);

                consumers.put(uri, consumer);
            } catch (IOException | URISyntaxException e1) {
                e1.printStackTrace();
                throw new RuntimeException("Cannot create: " + uri, e1);
            }
        }  else {
            if (LOGGER.isDebugEnabled()) LOGGER.debug("============ WARN ALREADY CREATED QQQQQQQQQQ:\n\t" + uri.toString() + " Recv: " + receiver);
        }
    }
    private String addressAsQueueName(URI endPoint) {
        return endPoint.toString().replace("//?", "/?");
    }
    @Override
    public void start() {
        started = true;

        try {
            rConfig.connect();
        } catch (Exception e) {
            LOGGER.error("Failed to connect to broker:" + e, e);
            e.printStackTrace();
        }
        LOGGER.info("CREATED CONNECTION:" + rConfig.getConnection());
    }

    @Override
    public void stop() {
        consumers.values().stream().forEach(consumer -> ((ClosableConsumer) consumer).close());
        try {
            rConfig.getConnection().close(10 * 1000);
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
class ClosableConsumer extends DefaultConsumer {

    private final String addressAsQueueName;
    private final Receiver receiver;

    public ClosableConsumer(String addressAsQueueName, Channel channel, Receiver receiver) {
            super(channel);

        this.addressAsQueueName = addressAsQueueName;
        this.receiver = receiver;
    }
    @Override
    public void handleDelivery (String consumerTag, Envelope envelope, AMQP.BasicProperties properties,  byte[] body)
            throws IOException {
            try {
                if (RabbitEndpointFactory.LOGGER.isDebugEnabled())
                    RabbitEndpointFactory.LOGGER.debug("============ RECEIVER INCOMING QQQQQQQQQQ:" + addressAsQueueName.toString());
                byte[] response = receiver.receive(body, "unknown", "unknown");
                if (response != null && response.length > 0) {
                    AMQP.BasicProperties.Builder builder = new AMQP.BasicProperties.Builder().correlationId(properties.getCorrelationId());
                    if (RabbitEndpointFactory.LOGGER.isDebugEnabled())
                        RabbitEndpointFactory.LOGGER.debug("============ RECEIVER SVR REPLYING:" + addressAsQueueName.toString() + "\n\t Reply:" + properties.getReplyTo() + ":" + properties.getCorrelationId() + " bytes[]" + response.length);
                    getChannel().basicPublish("", properties.getReplyTo(), builder.build(), response);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    public void close ()  {
        try {
            getChannel().close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}

