package com.yunzhi.mq.rocketmq.order;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

/**
 * @author zxb
 * @ClassName Producer
 * @Description 顺序消息的发送者 (局部消息一致)
 * @Date 2021/12/25 10:48
 * @Version 1.0
 */
public class Producer {

    public static void main(String[] args) throws Exception {
        // 创建生产者，并指定生产者的组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        // 指定 nameserver
        producer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 启动 producer
        producer.start();

        // 先构建消息
        List<OrderStep> orderSteps = OrderStep.buildOrders();

        // 发送消息
        for (int i = 0; i < orderSteps.size(); i++) {
            String body = JSON.toJSONString(orderSteps.get(i));
            System.out.println(body);
            Message message = new Message("orderTopic", "order", "i" + i, body.getBytes());
            /**
             * 我们要保证发送者发送的顺序和消费者消费的顺序是一样的，就要使用这个三个参数的 send 方法
             *
             * 第一个参数是 消息
             * 第二个参数是 消息队列选择器 在发送时要选择某一个队列进行发送
             * 第三个参数是 我们选择队列的一个业务标识 (传入订单 ID)
             */
            SendResult sendResult = producer.send(message, new MessageQueueSelector() {
                /**
                 *
                 * @param list 当前 Topic 下的所有队列集合，所有的消息队列都在这个集合中
                 * @param message 就是我们传递过来的 message 对象
                 * @param o 就是我们传过来的业务标识，订单 ID
                 * @return
                 */
                @Override
                public MessageQueue select(List<MessageQueue> list, Message message, Object o) {
                    long orderId = (long) o;
                    // 对消息队列数量进行取模
                    // 这样每次相同的订单 id 过来，都会放到相同的队列中去
                    long index = orderId % list.size();
                    return list.get((int) index);
                }
            }, orderSteps.get(i).getOrderId());
            System.out.println("发送结果" + sendResult);
        }

        // 关闭生产者
        producer.shutdown();
    }

}
