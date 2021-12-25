package com.yunzhi.mq.rocketmq.base.producer;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * @author zxb
 * @ClassName AsyncProducer
 * @Description 发送异步消息
 * @Date 2021/12/24 15:08
 * @Version 1.0
 */
public class AsyncProducer {

    public static void main(String[] args) {
        // 创建消息生产者，并指定组名
        DefaultMQProducer defaultMqProducer = null;
        try {
            defaultMqProducer = new DefaultMQProducer("group2");
            // 连接上 nameserver
            defaultMqProducer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
            // 启动 producer
            defaultMqProducer.start();
            // 创建消息
            for (int i = 0; i < 10; i++) {
                Message message = new Message("producer", "tag2", ("Hello RocketMQ" + i).getBytes());
                // 发送异步消息，消息结果在回调函数中处理
                defaultMqProducer.send(message, new SendCallback() {
                    /**
                     * 发送成功的回调函数
                     * @param sendResult 结果
                     */
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        System.out.println("发送结果" + sendResult);
                    }

                    /**
                     * 发送失败的回调函数
                     * @param throwable 结果
                     */
                    @Override
                    public void onException(Throwable throwable) {
                        System.out.println("错误信息" + throwable);
                    }
                });
            }
            // 让线程睡一秒，模拟线上延迟
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (MQClientException | RemotingException | InterruptedException e) {
            e.printStackTrace();
        }

    }

}
