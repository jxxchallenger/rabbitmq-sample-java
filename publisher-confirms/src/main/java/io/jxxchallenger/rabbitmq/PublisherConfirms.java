package io.jxxchallenger.rabbitmq;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class PublisherConfirms {

    private static final String EXCHANGE_NAME = "confirms-logs";
    
    private static final Logger LOGGER = LoggerFactory.getLogger(PublisherConfirms.class);
    
    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(ConnectionFactory.DEFAULT_HOST);
        try(Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT, false);
            channel.confirmSelect();
            
            //发布者确认--异步方式
            ConcurrentNavigableMap<Long, String> outstandingConfirms = new ConcurrentSkipListMap<>();
            ConfirmListener confirmListener = new ConfirmListenerImpl(outstandingConfirms);
            
            channel.addConfirmListener(confirmListener);
            String routingKey = getSeverity(args);
            String message = getMessage(args);
            outstandingConfirms.put(channel.getNextPublishSeqNo(), message);
            channel.basicPublish(EXCHANGE_NAME, routingKey, MessageProperties.TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
            //发布者确认--同步阻塞方式
            //channel.waitForConfirmsOrDie(5_000);
            LOGGER.info("[x] Sent '{}' : '{}'", routingKey, message);
            Thread.sleep(2000);
        }
    }
    
    private static String getSeverity(String[] strings) {
        if (strings.length < 1)
            return "info";
        return strings[0];
    }

    private static String getMessage(String[] strings) {
        if (strings.length < 2)
            return "Hello World!";
        return joinStrings(strings, " ", 1);
    }

    private static String joinStrings(String[] strings, String delimiter, int startIndex) {
        int length = strings.length;
        if (length == 0) return "";
        if (length <= startIndex) return "";
        StringBuilder words = new StringBuilder(strings[startIndex]);
        for (int i = startIndex + 1; i < length; i++) {
            words.append(delimiter).append(strings[i]);
        }
        return words.toString();
    }
}

class ConfirmListenerImpl implements ConfirmListener {

    private ConcurrentNavigableMap<Long, String> outstandingConfirms;
    
    public ConfirmListenerImpl() {
        super();
    }

    public ConfirmListenerImpl(ConcurrentNavigableMap<Long, String> outstandingConfirms) {
        super();
        this.outstandingConfirms = outstandingConfirms;
    }

    public ConcurrentNavigableMap<Long, String> getOutstandingConfirms() {
        return outstandingConfirms;
    }

    public void setOutstandingConfirms(ConcurrentNavigableMap<Long, String> outstandingConfirms) {
        this.outstandingConfirms = outstandingConfirms;
    }

    @Override
    public void handleAck(long deliveryTag, boolean multiple)
            throws IOException {
        if (multiple) {
            ConcurrentNavigableMap<Long, String> confirmed = outstandingConfirms.headMap(deliveryTag, true);
            confirmed.clear();
        } else {
            outstandingConfirms.remove(deliveryTag);
        }
    }

    @Override
    public void handleNack(long deliveryTag, boolean multiple)
            throws IOException {
        String body = outstandingConfirms.get(deliveryTag);
        System.err.format(
                "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
                body, deliveryTag, multiple
        );
        handleAck(deliveryTag, multiple);
    }
    
}
