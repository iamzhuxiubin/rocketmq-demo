package com.yunzhi.mq.rocketmq.batch;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author zxb
 * @ClassName Consumer
 * @Description TODO
 * @Date 2021/12/25 15:07
 * @Version 1.0
 */
public class Consumer {
    public static void main(String[] args) throws Exception {
        // 创建消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("batchConsumer");
        // 指定 nameserver 地址
        consumer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 指定订阅的主题和 tag
        consumer.subscribe("BatchTest","TagA");
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (Message message : list) {
                    System.out.println(message);
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("消费者启动");
    }
}
