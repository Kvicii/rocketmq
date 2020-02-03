/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    public static void main(String[] args) throws MQClientException {

        /*
         * Instantiate with specified consumer group name.
         */
//        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name");
//        开启消费端消息轨迹
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name", true);

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */

        /*
         * Specify where to start in case the specified consumer group is a brand new one.
         */
        consumer.setNamesrvAddr("localhost:9876");
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        /*
         * Subscribe one more more topics to consume.
         */
        consumer.subscribe("TopicTest", "*");
//        consumer.subscribe("TopicTest", "TagA || TagB");
//        consumer.subscribe("TopicTest", MessageSelector.bySql("a < 1"));

        /*
         *  Register callback to execute on arrival of messages fetched from brokers.
         */
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            for (MessageExt msg : msgs) {
                System.out.printf("Receive New Messages: Thread:%s, msg:%s, time:%s", Thread.currentThread().getName(), msg, System.currentTimeMillis() - msg.getStoreTimestamp() + "ms later");
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
            return ConsumeConcurrentlyStatus.RECONSUME_LATER;
        });

//        接收顺序消息
//        consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
//            context.setAutoCommit(true);
//            try {
//                for (int i = 0; i < msgs.size(); i++) {
//                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs.get(i));
//                    return ConsumeOrderlyStatus.SUCCESS;
//                }
//            } catch (Exception e) {

//                return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
//            }
//            return ConsumeOrderlyStatus.SUCCESS;
//        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();
        System.out.printf("Consumer Started.%n");
    }
}
