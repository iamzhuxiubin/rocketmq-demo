package com.yunzhi.mq.rocketmq.base.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author zxb
 * @ClassName Consumer
 * @Description 消息接收者
 * @Date 2021/12/24 16:14
 * @Version 1.0
 */
public class Consumer {
    public static void main(String[] args) throws MQClientException {
        // 1.创建消费者Consumer，并指定消费者组名
        // push 表示是 broker 主动给消费者推过去，也可以是消费者去主动拉消息
        DefaultMQPushConsumer defaultMqPushConsumer = new DefaultMQPushConsumer("group1");
        // 2.指定Nameserver地址
        defaultMqPushConsumer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 3.订阅主题Topic和Tag，这里订阅多个 tag 可以用 || 分开，如果要消费所有 tag，写个 * 也可以
        defaultMqPushConsumer.subscribe("producer", "tag1");

        // 在处理消息之前，还可以使用消费者的模式，有两种，一种是广播模式，一种是负载均衡模式
        defaultMqPushConsumer.setMessageModel(MessageModel.BROADCASTING);

        // 4.设置回调函数，处理消息
        defaultMqPushConsumer.registerMessageListener(new MessageListenerConcurrently() {
            // 接收消息内容的方法
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // 这个 list 就是消息的内容
                for (MessageExt messageExt : list) {
                    System.out.println(new String(messageExt.getBody()));
                }
                // 这个返回值就是： 消息消费完后返回的结果
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5.启动消费者consumer，消费者启动后就会去一直监听队列当中有没有消息
        defaultMqPushConsumer.start();
    }
}
