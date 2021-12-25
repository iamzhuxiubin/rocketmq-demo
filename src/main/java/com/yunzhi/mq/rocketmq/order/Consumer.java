package com.yunzhi.mq.rocketmq.order;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.List;

/**
 * @author zxb
 * @ClassName Consumer
 * @Description TODO
 * @Date 2021/12/25 11:33
 * @Version 1.0
 */
public class Consumer {

    public static void main(String[] args) throws Exception {
        // 创建消费者，并设置组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        // 连接 nameserver
        consumer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 订阅主题和tag
        consumer.subscribe("orderTopic", "*");
        // 注册消息监听器
        // 之前消息监听器用的传入的是 MessageListenerConcurrently ，这个是无序的
        // 而现在要保证 consumer 和 producer 消息的顺序一致，要传入 MessageListenerOrderly 对象
        // 使用 MessageListenerOrderly 那么就会去使用一个线程处理一个队列，保证顺序
        consumer.registerMessageListener(new MessageListenerOrderly() {
            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext consumeOrderlyContext) {
                list.forEach(e -> {
                    System.out.println("线程名称:【" + Thread.currentThread().getName() + "】,消费消息:" + new String(e.getBody()));
                });
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });
        // 启动消费者
        consumer.start();
        System.out.println("消费者启动");
    }

}
