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

public class ReceiveLogs {

    private static final String EXCHANGE_NAME = "logs";
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ReceiveLogs.class);
    
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ConnectionFactory.DEFAULT_HOST);
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.FANOUT, false);
        String queueName = channel.queueDeclare().getQueue();
        channel.queueBind(queueName, EXCHANGE_NAME, "");
        channel.basicQos(1);
        LOGGER.info(" [*] Waiting for messages. To exit press CTRL+C");
        
        DeliverCallback callback = (consumerTag, deliver) -> {
            String message = new String(deliver.getBody(), StandardCharsets.UTF_8);
            try {
                LOGGER.info("[x] Received '{}'", message);
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            finally {
                channel.basicAck(deliver.getEnvelope().getDeliveryTag(), false);
            }
        };
        channel.basicConsume(queueName, false, callback, consumerTag -> {});
    }

}
