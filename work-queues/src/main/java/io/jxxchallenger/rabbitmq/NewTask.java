package io.jxxchallenger.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class NewTask {

    private static final String QUEUE_NAME = "task_queue";
    
    private static final Logger LOGGER = LoggerFactory.getLogger(NewTask.class);
    
    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try(Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            //channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);//队列持久化
            //String message = String.join(" ", args);
            StringBuilder sb = new StringBuilder("message");
            for(int i = 1; i <= 10 ; i ++) {
                sb.append(".");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //channel.basicPublish("", QUEUE_NAME, MessageProperties.TEXT_PLAIN, sb.toString().getBytes(StandardCharsets.UTF_8));
                //消息持久化
                channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, sb.toString().getBytes(StandardCharsets.UTF_8));
                LOGGER.info(" [x] Sent '{}'", sb.toString());
            }
            
        }
    }

}
