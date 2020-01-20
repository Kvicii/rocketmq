package org.apache.rocketmq.test.transaction;

import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author kyushu
 * @date 2020/1/20 16:28
 * @description 事务消息生产者
 */
public class TransactionProducer {

    public static void main(String[] args) throws Exception {

        TransactionListener transactionListener = new TransactionListenerImpl();
        TransactionMQProducer producer = new TransactionMQProducer();
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(5, 10, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2000), r -> {
            Thread thread = new Thread();
            thread.setName("client-transaction-msg-check-thread");
            return thread;
        });
        producer.setExecutorService(threadPoolExecutor);
        producer.setTransactionListener(transactionListener);
        producer.start();

        String[] tags = new String[]{"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            Message message = new Message("TopicTest1234", tags[i % tags.length], "KEY:" + i, ("Hello RocketMQ " + i).getBytes());
            TransactionSendResult sendResult = producer.sendMessageInTransaction(message, null);
        }
    }
}
