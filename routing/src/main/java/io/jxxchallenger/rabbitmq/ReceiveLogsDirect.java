package io.jxxchallenger.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class ReceiveLogsDirect {

    private static final String EXCHANGE_NAME = "direct_logs";
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveLogsDirect.class);
    
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ConnectionFactory.DEFAULT_HOST);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.basicQos(1);
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, false);
        String queueName = channel.queueDeclare().getQueue();
        for(String bind:args) {
            channel.queueBind(queueName, EXCHANGE_NAME, bind);
        }
        LOGGER.info("[*] Waiting for messages. To exit press CTRL+C");
        
        DeliverCallback callback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                LOGGER.info("[x] Received '{}':'{}'", delivery.getEnvelope().getRoutingKey(), message);
            }
            finally {
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
        };
        
        channel.basicConsume(queueName, false, callback, consumerTag -> {});
    }
}
