package com.yunzhi.mq.rocketmq.batch;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zxb
 * @ClassName Producer
 * @Description TODO
 * @Date 2021/12/25 15:03
 * @Version 1.0
 */
public class Producer {

    public static void main(String[] args) throws MQClientException {
        // 创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("batchGroup");
        // 指定 nameserver
        producer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 启动生产者
        producer.start();
        // 创建消息
        String topic = "BatchTest";
        List<Message> messages = new ArrayList<>();
        messages.add(new Message(topic, "TagA", "OrderID001", "Hello world 0".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderID002", "Hello world 1".getBytes()));
        messages.add(new Message(topic, "TagA", "OrderID003", "Hello world 2".getBytes()));
        // 发送消息
        try {
            producer.send(messages);
        } catch (Exception e) {
            e.printStackTrace();
        }
        // 关闭消息
        producer.shutdown();

    }

}
