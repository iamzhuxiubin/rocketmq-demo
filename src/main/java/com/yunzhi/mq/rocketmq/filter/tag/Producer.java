package com.yunzhi.mq.rocketmq.filter.tag;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zxb
 * @ClassName Producer
 * @Description tag filter producer
 * @Date 2021/12/25 15:43
 * @Version 1.0
 */
public class Producer {
    public static void main(String[] args) throws Exception {
        DefaultMQProducer producer = new DefaultMQProducer("TagFilterConsumer");
        producer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        producer.start();
        List<Message> messageList = new ArrayList<>();
        Message message1 = new Message("TopicA", "TagA", "hello message A".getBytes());
        Message message2 = new Message("TopicA", "TagB", "hello message B".getBytes());
        Message message3 = new Message("TopicA", "TagC", "hello message C".getBytes());
        messageList.add(message1);
        messageList.add(message2);
        messageList.add(message3);
        producer.send(messageList);
        producer.shutdown();
    }
}
