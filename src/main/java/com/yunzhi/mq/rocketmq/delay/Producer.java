package com.yunzhi.mq.rocketmq.delay;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;


/**
 * @author zxb
 * @ClassName Producer
 * @Description TODO
 * @Date 2021/12/25 13:17
 * @Version 1.0
 */
public class Producer {

    public static void main(String[] args) throws Exception {
        // Instantiate a producer to send scheduled messages
        DefaultMQProducer producer = new DefaultMQProducer("ExampleProducerGroup");
        producer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // Launch producer
        producer.start();
        int totalMessagesToSend = 10;
        for (int i = 0; i < totalMessagesToSend; i++) {
            Message message = new Message("TestTopic", ("Hello scheduled message " + i).getBytes());
            // This message will be delivered to consumer 10 seconds later.
            message.setDelayTimeLevel(3);
            // Send the message
            SendResult send = producer.send(message);
            System.out.println("发送结果为" + send);
        }

        // Shutdown producer after use.
        producer.shutdown();
    }

}
