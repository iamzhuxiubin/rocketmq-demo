package com.yunzhi.mq.rocketmq.transaction;

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
 * @Date 2021/12/25 17:20
 * @Version 1.0
 */
public class Consumer {

    public static void main(String[] args) throws Exception {
        // 创建消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("transactionConsumer");
        // 设置 nameserver 地址
        consumer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 订阅主题和二级主题
        consumer.subscribe("TransactionTopic", "TagA || TagB || TagC");
        // 监听回调
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.println("线程为:" + Thread.currentThread().getName() + ",消息为:" + new String(messageExt.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 开启消费者
        consumer.start();
        System.out.println("消费者启动");
    }

}
