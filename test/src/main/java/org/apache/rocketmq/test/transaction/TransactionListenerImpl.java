package org.apache.rocketmq.test.transaction;

import com.google.common.collect.Maps;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kyushu
 * @date 2020/1/20 16:27
 * @description 事务消息回调接口
 */
public class TransactionListenerImpl implements TransactionListener {

    private AtomicInteger transactionIndex = new AtomicInteger(0);

    private Map<String, Integer> localTransaction = Maps.newConcurrentMap();

    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        int value = transactionIndex.getAndIncrement();
        int status = value % 3;
        localTransaction.put(msg.getTransactionId(), status);
        return LocalTransactionState.UNKNOW;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        String transactionId = msg.getTransactionId();
        Integer status = localTransaction.get(transactionId);
        if (null != status) {
            switch (status) {
                case 0:
                    return LocalTransactionState.UNKNOW;
                case 1:
                    return LocalTransactionState.COMMIT_MESSAGE;
                case 2:
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                default:
                    return LocalTransactionState.COMMIT_MESSAGE;
            }
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }
}
