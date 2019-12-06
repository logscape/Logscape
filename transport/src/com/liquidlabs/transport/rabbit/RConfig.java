package com.liquidlabs.transport.rabbit;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class RConfig {
    private final String brokerUri;

    private ConnectionFactory factory;
    private Connection connection;


    /**
     * Like: "amqp://admin:admin@localhost:5672"
     * @param brokerUrl
     */
    public RConfig(String brokerUrl){
        this.brokerUri = brokerUrl;
    }
    public RConfig(String broker, int port, String username, String password){
        this(String.format("amqp://%s:%s@%s:%d", username, password, broker, port));
        }
        public void connect() throws IOException, TimeoutException, URISyntaxException, KeyManagementException, NoSuchAlgorithmException {
            factory = new ConnectionFactory();
            URI uri = new URI(brokerUri);
            factory.setUri(uri);
            connection = factory.newConnection();

            if (!connection.isOpen()) {
                throw new RuntimeException("Failed to connect to:" + uri.getHost() + " port:" + uri.getPort());
            }
        }
        public String getBrokerUri() {
            return brokerUri;
        }

        public Channel getChannel(String queueName) throws IOException, URISyntaxException {
            Channel channel = connection.createChannel();
            if (!channel.isOpen()) {
                URI uri = new URI(brokerUri);
                throw new RuntimeException("Failed to create Channel on:" + uri.getHost() + " port:" + uri.getPort());
            }

            channel.queueDeclare(queueName, false, false, false, null);
            return channel;
        }

    public Connection getConnection() {
        return connection;
    }
}
