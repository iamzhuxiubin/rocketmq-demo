package com.yunzhi.mq.rocketmq.filter.sql;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author zxb
 * @ClassName Consumer
 * @Description tag filter consumer
 * @Date 2021/12/25 15:43
 * @Version 1.0
 */
public class Consumer {
    public static void main(String[] args) throws Exception {
        // 创建消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("SQLFilter");
        // 指定 nameserver
        consumer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 设置订阅的主题和二级主题
        // 可以用 * 来表示消费所有 tag
        consumer.subscribe("SQLFilterTopic", MessageSelector.bySql("a between 0 and 3"));
        // 添加监听
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.println(new String(messageExt.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        consumer.start();
        System.out.println("消费者已启动");
    }
}
