package com.yunzhi.mq.rocketmq.filter.sql;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;

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
        // 创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("SQLFilterConsumer");
        // 设置 nameserver
        producer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 启动生产者
        producer.start();

        // 创建消息
        int sendTotal = 10;
        for (int i = 0; i < sendTotal; i++) {
            Message message = new Message("SQLFilterTopic", "tag1", ("Hello SQLFilter" + i).getBytes());
            // 设置一些自定义的值，用来给 consumer 进行 sql 过滤
            message.putUserProperty("a", String.valueOf(i));
            SendResult send = producer.send(message);
            System.out.println(send);
        }

        // 关闭生产者
        producer.shutdown();
    }
}
