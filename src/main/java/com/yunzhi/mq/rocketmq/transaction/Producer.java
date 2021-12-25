package com.yunzhi.mq.rocketmq.transaction;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author zxb
 * @ClassName Producer
 * @Description TODO
 * @Date 2021/12/25 17:20
 * @Version 1.0
 */
public class Producer {

    public static void main(String[] args) throws Exception {

        TransactionMQProducer transactionMqProducer = new TransactionMQProducer("transaction");
        // 设置 nameserver 地址
        transactionMqProducer.setNamesrvAddr("192.168.191.131:9876;192.168.191.132:9876");
        // 设置事务监听器，用于检测事务提交和回滚，以及回查
        transactionMqProducer.setTransactionListener(new TransactionListener() {
            /**
             * 在该方法中执行本地事务
             * @param message
             * @param o
             * @return
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message message, Object o) {
                // 如果 tag 是 Tag1 ，那么就提交
                if (StringUtils.equals("TagA", message.getTags())) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else if (StringUtils.equals("TagB", message.getTags())) {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                } else {
                    // 如果不做处理，那么就将在回查方法中进行处理
                    return LocalTransactionState.UNKNOW;
                }
            }

            /**
             * 该方法用于 mq 进行事务状态的回查，因为有可能网络原因或者阻塞啥的一直没有提交事务
             * @param messageExt
             * @return
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
                System.out.println("看看回查的消息是不是 TagC");
                System.out.println(messageExt);
                // 提交
                return LocalTransactionState.COMMIT_MESSAGE;
            }
        });
        // 启动生产者
        transactionMqProducer.start();
        String[] tags = {"TagA","TagB","TagC"};
        int sendTotal = tags.length;
        for (int i = 0; i < sendTotal; i++) {
            // 通过不同的 tag 来区分不同的消息
            Message message = new Message("TransactionTopic", tags[i], ("hello transaction message" + i).getBytes());
            // 发送消息要在事务中发送，此时发送的都是半消息
            TransactionSendResult sendResult = transactionMqProducer.sendMessageInTransaction(message, null);
            System.out.println("发送结果:" + sendResult);
        }
        // 由于 mq 要回查消息生产者，所以就不要关闭了
        //transactionMqProducer.shutdown();
    }
}
