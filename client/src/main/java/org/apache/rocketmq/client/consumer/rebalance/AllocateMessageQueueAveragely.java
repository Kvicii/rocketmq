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
package org.apache.rocketmq.client.consumer.rebalance;

import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.ArrayList;
import java.util.List;

/**
 * Average Hashing queue algorithm
 * <p>
 * 平均分发ConsumeQueue 默认的负载均衡策略
 */
public class AllocateMessageQueueAveragely implements AllocateMessageQueueStrategy {
	private final InternalLogger log = ClientLogger.getLog();

	/**
	 * 将MessageQueue分配给当前Consumer
	 * MessageQueue在同一Consumer Group不同Consumer之间的负载均衡
	 * 其核心设计理念是在一个MessageQueue在同一时间只允许被同一Consumer Group内的一个Consumer消费 一个Consumer能同时消费多个MessageQueue
	 *
	 * @param consumerGroup current consumer group
	 *                      消费者组
	 * @param currentCID    current consumer id
	 *                      当前Consumer
	 * @param mqAll         message queue set in current topic
	 *                      当前Topic下的所有MessageQueue
	 * @param cidAll        consumer set in current consumer group
	 *                      当前Topic下的所有Consumer Id
	 * @return
	 */
	@Override
	public List<MessageQueue> allocate(String consumerGroup, String currentCID, List<MessageQueue> mqAll,
									   List<String> cidAll) {
		if (currentCID == null || currentCID.length() < 1) {
			throw new IllegalArgumentException("currentCID is empty");
		}
		if (mqAll == null || mqAll.isEmpty()) {
			throw new IllegalArgumentException("mqAll is null or mqAll empty");
		}
		if (cidAll == null || cidAll.isEmpty()) {
			throw new IllegalArgumentException("cidAll is null or cidAll empty");
		}

		List<MessageQueue> result = new ArrayList<MessageQueue>();
		if (!cidAll.contains(currentCID)) {
			log.info("[BUG] ConsumerGroup: {} The consumerId: {} not in cidAll: {}",
					consumerGroup,
					currentCID,
					cidAll);
			return result;
		}
		// 获取当前消费者在cidAll中的索引
		int index = cidAll.indexOf(currentCID);
		// 取模 如果有剩余MessageQueue 需要将多出来的MessageQueue分配给第一些Consumer
		int mod = mqAll.size() % cidAll.size();
		// 计算每个Consumer应该被均分到的MessageQueue
		// 1.MessageQueue的数量 <= Consumer的数量 说明每个Consumer最多只能消费到一个MessageQueue 直接按一个分配
		// 2.如果 mod > 0 并且当前 index < mod 则当前Consumer应该消费的MessageQueue的个数为 mqAll.size() / cidAll.size() + 1
		// 否则当前Consumer应该消费的MessageQueue个数为 mqAll.size() / cidAll.size()
		// 2.中只是为了体现均分的思想 如8个MessageQueue 3个消费者 模数为2 如果当前Consumer Index为1 则他应该分配到MessageQueue就是2 + 1 如果Consumer Index 为2
		int averageSize = mqAll.size() <= cidAll.size() ? 1 :
				(mod > 0 && index < mod ? mqAll.size() / cidAll.size() + 1 : mqAll.size() / cidAll.size());
		// 计算分配给当前Consumer的MessageQueue的第一个Index
		int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
		// 确定分配给当前MessageQueue的个数
		int range = Math.min(averageSize, mqAll.size() - startIndex);
		for (int i = 0; i < range; i++) {   // 轮询的放入结果 分配过程最后的计算十分巧妙
			result.add(mqAll.get((startIndex + i) % mqAll.size()));
		}
		return result;
	}

	@Override
	public String getName() {
		return "AVG";
	}
}
