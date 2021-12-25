package com.yunzhi.mq.rocketmq.base.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author zxb
 * @ClassName SyncProducer
 * @Description 发送同步消息
 * @Date 2021/12/24 14:37
 * @Version 1.0
 */
public class SyncProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 1.创建消息生产者producer，并制定生产者组名
        //  使用默认的生产者
        DefaultMQProducer defaultMqProducer = new DefaultMQProducer("group1");
        // 2.指定Nameserver地址，集群环境，用逗号隔开
        defaultMqProducer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 3.启动producer
        defaultMqProducer.start();
        // 4.创建消息对象，指定主题Topic、Tag和消息体
        for (int i = 0; i < 10; i++) {
            // 指定主题Topic、Tag和消息体
            Message message = new Message("producer", "tag1", ("Hello World 哈" + i).getBytes());
            // 5.发送同步消息
            SendResult send = defaultMqProducer.send(message);
            // 主节点用于接收生产者发送过来的消息，所以都是主 broker 来接收消息
            // 从 broker 用来被 consumer 读取消息
            System.out.println("发送结果" + send);
            // // 让线程睡一秒，模拟线上延迟
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        // 6.关闭生产者producer
        defaultMqProducer.shutdown();
    }


}
