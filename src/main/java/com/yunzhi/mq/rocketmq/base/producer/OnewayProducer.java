package com.yunzhi.mq.rocketmq.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author zxb
 * @ClassName OnewayProducer  单向发送消息
 * @Description 这种方式主要用在不特别关心发送结果的场景，例如日志发送。
 * @Date 2021/12/24 15:36
 * @Version 1.0
 */
public class OnewayProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 创建 producer
        DefaultMQProducer defaultMqProducer = new DefaultMQProducer("group3");
        // 连接 nameserver
        defaultMqProducer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 启动 producer
        defaultMqProducer.start();
        // 创建消息，指定 topic，tag，message body
        for (int i = 0; i < 10; i++) {
            Message message = new Message("producer", "tag3", ("Hello OneWay Produce" + i).getBytes());
            // 发送单向消息，没有返回值
            defaultMqProducer.sendOneway(message);
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        // 关闭生产者
        defaultMqProducer.shutdown();
    }

}
