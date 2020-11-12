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
package org.apache.rocketmq.broker;

import org.apache.rocketmq.acl.AccessValidator;
import org.apache.rocketmq.broker.client.ClientHousekeepingService;
import org.apache.rocketmq.broker.client.ConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ConsumerManager;
import org.apache.rocketmq.broker.client.DefaultConsumerIdsChangeListener;
import org.apache.rocketmq.broker.client.ProducerManager;
import org.apache.rocketmq.broker.client.net.Broker2Client;
import org.apache.rocketmq.broker.client.rebalance.RebalanceLockManager;
import org.apache.rocketmq.broker.dledger.DLedgerRoleChangeHandler;
import org.apache.rocketmq.broker.filter.CommitLogDispatcherCalcBitMap;
import org.apache.rocketmq.broker.filter.ConsumerFilterManager;
import org.apache.rocketmq.broker.filtersrv.FilterServerManager;
import org.apache.rocketmq.broker.latency.BrokerFastFailure;
import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.broker.longpolling.NotifyMessageArrivingListener;
import org.apache.rocketmq.broker.longpolling.PullRequestHoldService;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.broker.offset.ConsumerOffsetManager;
import org.apache.rocketmq.broker.out.BrokerOuterAPI;
import org.apache.rocketmq.broker.plugin.MessageStoreFactory;
import org.apache.rocketmq.broker.plugin.MessageStorePluginContext;
import org.apache.rocketmq.broker.processor.AdminBrokerProcessor;
import org.apache.rocketmq.broker.processor.ClientManageProcessor;
import org.apache.rocketmq.broker.processor.ConsumerManageProcessor;
import org.apache.rocketmq.broker.processor.EndTransactionProcessor;
import org.apache.rocketmq.broker.processor.PullMessageProcessor;
import org.apache.rocketmq.broker.processor.QueryMessageProcessor;
import org.apache.rocketmq.broker.processor.ReplyMessageProcessor;
import org.apache.rocketmq.broker.processor.SendMessageProcessor;
import org.apache.rocketmq.broker.slave.SlaveSynchronize;
import org.apache.rocketmq.broker.subscription.SubscriptionGroupManager;
import org.apache.rocketmq.broker.topic.TopicConfigManager;
import org.apache.rocketmq.broker.transaction.AbstractTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.TransactionalMessageCheckService;
import org.apache.rocketmq.broker.transaction.TransactionalMessageService;
import org.apache.rocketmq.broker.transaction.queue.DefaultTransactionalMessageCheckListener;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageBridge;
import org.apache.rocketmq.broker.transaction.queue.TransactionalMessageServiceImpl;
import org.apache.rocketmq.broker.util.ServiceProvider;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import org.apache.rocketmq.common.stats.MomentStatsItem;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.RemotingServer;
import org.apache.rocketmq.remoting.common.TlsMode;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingServer;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.netty.NettyServerConfig;
import org.apache.rocketmq.remoting.netty.RequestTask;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.srvutil.FileWatchService;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageArrivingListener;
import org.apache.rocketmq.store.MessageStore;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.stats.BrokerStats;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BrokerController {
	private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
	private static final InternalLogger LOG_PROTECTION = InternalLoggerFactory.getLogger(LoggerName.PROTECTION_LOGGER_NAME);
	private static final InternalLogger LOG_WATER_MARK = InternalLoggerFactory.getLogger(LoggerName.WATER_MARK_LOGGER_NAME);
	private final BrokerConfig brokerConfig;
	private final NettyServerConfig nettyServerConfig;
	private final NettyClientConfig nettyClientConfig;
	private final MessageStoreConfig messageStoreConfig;
	private final ConsumerOffsetManager consumerOffsetManager;
	private final ConsumerManager consumerManager;
	private final ConsumerFilterManager consumerFilterManager;
	private final ProducerManager producerManager;
	private final ClientHousekeepingService clientHousekeepingService;
	private final PullMessageProcessor pullMessageProcessor;
	private final PullRequestHoldService pullRequestHoldService;
	private final MessageArrivingListener messageArrivingListener;
	private final Broker2Client broker2Client;
	private final SubscriptionGroupManager subscriptionGroupManager;
	private final ConsumerIdsChangeListener consumerIdsChangeListener;
	private final RebalanceLockManager rebalanceLockManager = new RebalanceLockManager();
	private final BrokerOuterAPI brokerOuterAPI;
	private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl(
			"BrokerControllerScheduledThread"));
	private final SlaveSynchronize slaveSynchronize;
	private final BlockingQueue<Runnable> sendThreadPoolQueue;
	private final BlockingQueue<Runnable> pullThreadPoolQueue;
	private final BlockingQueue<Runnable> replyThreadPoolQueue;
	private final BlockingQueue<Runnable> queryThreadPoolQueue;
	private final BlockingQueue<Runnable> clientManagerThreadPoolQueue;
	private final BlockingQueue<Runnable> heartbeatThreadPoolQueue;
	private final BlockingQueue<Runnable> consumerManagerThreadPoolQueue;
	private final BlockingQueue<Runnable> endTransactionThreadPoolQueue;
	private final FilterServerManager filterServerManager;
	private final BrokerStatsManager brokerStatsManager;
	private final List<SendMessageHook> sendMessageHookList = new ArrayList<>();
	private final List<ConsumeMessageHook> consumeMessageHookList = new ArrayList<>();
	private MessageStore messageStore;
	private RemotingServer remotingServer;
	private RemotingServer fastRemotingServer;
	private TopicConfigManager topicConfigManager;
	private ExecutorService sendMessageExecutor;
	private ExecutorService pullMessageExecutor;
	private ExecutorService replyMessageExecutor;
	private ExecutorService queryMessageExecutor;
	private ExecutorService adminBrokerExecutor;
	private ExecutorService clientManageExecutor;
	private ExecutorService heartbeatExecutor;
	private ExecutorService consumerManageExecutor;
	private ExecutorService endTransactionExecutor;
	private boolean updateMasterHAServerAddrPeriodically = false;
	private BrokerStats brokerStats;
	private InetSocketAddress storeHost;
	private BrokerFastFailure brokerFastFailure;
	private Configuration configuration;
	private FileWatchService fileWatchService;
	private TransactionalMessageCheckService transactionalMessageCheckService;
	private TransactionalMessageService transactionalMessageService;
	private AbstractTransactionalMessageCheckListener transactionalMessageCheckListener;
	private Future<?> slaveSyncFuture;
	private Map<Class, AccessValidator> accessValidatorMap = new HashMap<>();

	public BrokerController(
			final BrokerConfig brokerConfig,
			final NettyServerConfig nettyServerConfig,
			final NettyClientConfig nettyClientConfig,
			final MessageStoreConfig messageStoreConfig
	) {
		/**
		 * 填充核心配置类
		 */
		this.brokerConfig = brokerConfig;
		this.nettyServerConfig = nettyServerConfig;
		this.nettyClientConfig = nettyClientConfig;
		this.messageStoreConfig = messageStoreConfig;
		/**
		 * 初始化核心配置组件
		 * consumerOffsetManager-->管理消息消费offset
		 * pullMessageProcessor-->consumer请求消息拉取管理
		 */
		this.consumerOffsetManager = new ConsumerOffsetManager(this);
		this.topicConfigManager = new TopicConfigManager(this);
		this.pullMessageProcessor = new PullMessageProcessor(this);
		this.pullRequestHoldService = new PullRequestHoldService(this);
		this.messageArrivingListener = new NotifyMessageArrivingListener(this.pullRequestHoldService);
		this.consumerIdsChangeListener = new DefaultConsumerIdsChangeListener(this);
		this.consumerManager = new ConsumerManager(this.consumerIdsChangeListener);
		this.consumerFilterManager = new ConsumerFilterManager(this);
		this.producerManager = new ProducerManager();
		this.clientHousekeepingService = new ClientHousekeepingService(this);
		this.broker2Client = new Broker2Client(this);
		this.subscriptionGroupManager = new SubscriptionGroupManager(this);
		this.brokerOuterAPI = new BrokerOuterAPI(nettyClientConfig);
		this.filterServerManager = new FilterServerManager(this);
		this.slaveSynchronize = new SlaveSynchronize(this);
		/**
		 * 实现上述组件功能的后台线程池 实际是由这些线程池处理
		 */
		this.sendThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getSendThreadPoolQueueCapacity());
		this.pullThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getPullThreadPoolQueueCapacity());
		this.replyThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getReplyThreadPoolQueueCapacity());
		this.queryThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getQueryThreadPoolQueueCapacity());
		this.clientManagerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getClientManagerThreadPoolQueueCapacity());
		this.consumerManagerThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getConsumerManagerThreadPoolQueueCapacity());
		this.heartbeatThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getHeartbeatThreadPoolQueueCapacity());
		this.endTransactionThreadPoolQueue = new LinkedBlockingQueue<>(this.brokerConfig.getEndTransactionPoolQueueCapacity());
		/**
		 * 统计组件和处理broker故障的组件
		 */
		this.brokerStatsManager = new BrokerStatsManager(this.brokerConfig.getBrokerClusterName());
		this.setStoreHost(new InetSocketAddress(this.getBrokerConfig().getBrokerIP1(), this.getNettyServerConfig().getListenPort()));
		this.brokerFastFailure = new BrokerFastFailure(this);
		this.configuration = new Configuration(
				log,
				BrokerPathConfigHelper.getBrokerConfigPath(),
				this.brokerConfig, this.nettyServerConfig, this.nettyClientConfig, this.messageStoreConfig
		);
	}

	public BrokerConfig getBrokerConfig() {
		return brokerConfig;
	}

	public NettyServerConfig getNettyServerConfig() {
		return nettyServerConfig;
	}

	public BlockingQueue<Runnable> getPullThreadPoolQueue() {
		return pullThreadPoolQueue;
	}

	public BlockingQueue<Runnable> getQueryThreadPoolQueue() {
		return queryThreadPoolQueue;
	}

	public boolean initialize() throws CloneNotSupportedException {

		// 加载topicConfig consumeOffset consumer订阅组 过滤器 都加载成功结果为true
		boolean result = this.topicConfigManager.load();
		result = result && this.consumerOffsetManager.load();
		result = result && this.subscriptionGroupManager.load();
		result = result && this.consumerFilterManager.load();

		if (result) {
			try {
				// 创建消息存储组件
				this.messageStore =
						new DefaultMessageStore(this.messageStoreConfig, this.brokerStatsManager, this.messageArrivingListener,
								this.brokerConfig);
				// 如果启用了Dleger技术进行主从同步和管理commitlog需要进行对Dleger的相关组件进行初始化
				if (messageStoreConfig.isEnableDLegerCommitLog()) {
					DLedgerRoleChangeHandler roleChangeHandler = new DLedgerRoleChangeHandler(this, (DefaultMessageStore) messageStore);
					((DLedgerCommitLog) ((DefaultMessageStore) messageStore).getCommitLog()).getdLedgerServer().getdLedgerLeaderElector().addRoleChangeHandler(roleChangeHandler);
				}
				// 初始化broker统计组件
				this.brokerStats = new BrokerStats((DefaultMessageStore) this.messageStore);
				// load plugin
				MessageStorePluginContext context = new MessageStorePluginContext(messageStoreConfig, brokerStatsManager, messageArrivingListener, brokerConfig);
				this.messageStore = MessageStoreFactory.build(context, this.messageStore);
				this.messageStore.getDispatcherList().addFirst(new CommitLogDispatcherCalcBitMap(this.brokerConfig, this.consumerFilterManager));
			} catch (IOException e) {
				result = false;
				log.error("Failed to initialize", e);
			}
		}
		result = result && this.messageStore.load();

		if (result) {
			// 初始化netty网络服务器
			this.remotingServer = new NettyRemotingServer(this.nettyServerConfig, this.clientHousekeepingService);
			NettyServerConfig fastConfig = (NettyServerConfig) this.nettyServerConfig.clone();
			fastConfig.setListenPort(nettyServerConfig.getListenPort() - 2);
			this.fastRemotingServer = new NettyRemotingServer(fastConfig, this.clientHousekeepingService);

			/**
			 * 初始化各类线程池及后台定时任务
			 */
			// 处理消息发送过来的线程池
			this.sendMessageExecutor = new BrokerFixedThreadPoolExecutor(
					this.brokerConfig.getSendMessageThreadPoolNums(),
					this.brokerConfig.getSendMessageThreadPoolNums(),
					1000 * 60,
					TimeUnit.MILLISECONDS,
					this.sendThreadPoolQueue,
					new ThreadFactoryImpl("SendMessageThread_"));
			// consumer消息拉取线程池
			this.pullMessageExecutor = new BrokerFixedThreadPoolExecutor(
					this.brokerConfig.getPullMessageThreadPoolNums(),
					this.brokerConfig.getPullMessageThreadPoolNums(),
					1000 * 60,
					TimeUnit.MILLISECONDS,
					this.pullThreadPoolQueue,
					new ThreadFactoryImpl("PullMessageThread_"));
			// 回复消息线程池
			this.replyMessageExecutor = new BrokerFixedThreadPoolExecutor(
					this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
					this.brokerConfig.getProcessReplyMessageThreadPoolNums(),
					1000 * 60,
					TimeUnit.MILLISECONDS,
					this.replyThreadPoolQueue,
					new ThreadFactoryImpl("ProcessReplyMessageThread_"));
			// 消息查询线程池
			this.queryMessageExecutor = new BrokerFixedThreadPoolExecutor(
					this.brokerConfig.getQueryMessageThreadPoolNums(),
					this.brokerConfig.getQueryMessageThreadPoolNums(),
					1000 * 60,
					TimeUnit.MILLISECONDS,
					this.queryThreadPoolQueue,
					new ThreadFactoryImpl("QueryMessageThread_"));
			// 管理broker命令执行线程池
			this.adminBrokerExecutor =
					Executors.newFixedThreadPool(this.brokerConfig.getAdminBrokerThreadPoolNums(), new ThreadFactoryImpl(
							"AdminBrokerThread_"));
			// 管理客户端线程池
			this.clientManageExecutor = new ThreadPoolExecutor(
					this.brokerConfig.getClientManageThreadPoolNums(),
					this.brokerConfig.getClientManageThreadPoolNums(),
					1000 * 60,
					TimeUnit.MILLISECONDS,
					this.clientManagerThreadPoolQueue,
					new ThreadFactoryImpl("ClientManageThread_"));
			// 负责给namesrv发送心跳消息的线程池
			this.heartbeatExecutor = new BrokerFixedThreadPoolExecutor(
					this.brokerConfig.getHeartbeatThreadPoolNums(),
					this.brokerConfig.getHeartbeatThreadPoolNums(),
					1000 * 60,
					TimeUnit.MILLISECONDS,
					this.heartbeatThreadPoolQueue,
					new ThreadFactoryImpl("HeartbeatThread_", true));
			// 结束事务线程池
			this.endTransactionExecutor = new BrokerFixedThreadPoolExecutor(
					this.brokerConfig.getEndTransactionThreadPoolNums(),
					this.brokerConfig.getEndTransactionThreadPoolNums(),
					1000 * 60,
					TimeUnit.MILLISECONDS,
					this.endTransactionThreadPoolQueue,
					new ThreadFactoryImpl("EndTransactionThread_"));
			// 管理消费端线程池
			this.consumerManageExecutor =
					Executors.newFixedThreadPool(this.brokerConfig.getConsumerManageThreadPoolNums(), new ThreadFactoryImpl(
							"ConsumerManageThread_"));
			this.registerProcessor();
			/**
			 * 定时调度的后台线程
			 */
			final long initialDelay = UtilAll.computeNextMorningTimeMillis() - System.currentTimeMillis();
			final long period = 1000 * 60 * 60 * 24;
			// broker的定时统计任务
			this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				try {
					BrokerController.this.getBrokerStats().record();
				} catch (Throwable e) {
					log.error("schedule record error.", e);
				}
			}, initialDelay, period, TimeUnit.MILLISECONDS);
			// consumer消费 offset持久化到磁盘的定时任务
			this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				try {
					BrokerController.this.consumerOffsetManager.persist();
				} catch (Throwable e) {
					log.error("schedule persist consumerOffset error.", e);
				}
			}, 1000 * 10, this.brokerConfig.getFlushConsumerOffsetInterval(), TimeUnit.MILLISECONDS);
			// 对consumer filter过滤器定时持久化的任务
			this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				try {
					BrokerController.this.consumerFilterManager.persist();
				} catch (Throwable e) {
					log.error("schedule persist consumer filter error.", e);
				}
			}, 1000 * 10, 1000 * 10, TimeUnit.MILLISECONDS);
			// broker的定时保护任务
			this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				try {
					BrokerController.this.protectBroker();
				} catch (Throwable e) {
					log.error("protectBroker error.", e);
				}
			}, 3, 3, TimeUnit.MINUTES);
			// 定时打印水位任务
			this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				try {
					BrokerController.this.printWaterMark();
				} catch (Throwable e) {
					log.error("printWaterMark error.", e);
				}
			}, 10, 1, TimeUnit.SECONDS);
			// 落后的commitlog定时分发的任务
			this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				try {
					log.info("dispatch behind commit log {} bytes", BrokerController.this.getMessageStore().dispatchBehindBytes());
				} catch (Throwable e) {
					log.error("schedule dispatchBehindBytes error.", e);
				}
			}, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
			// 设置namesrv地址列表 支持不通过配置的方式写入namesrv地址 还可以发送请求去加载地址
			if (this.brokerConfig.getNamesrvAddr() != null) {
				this.brokerOuterAPI.updateNameServerAddressList(this.brokerConfig.getNamesrvAddr());
				log.info("Set user specified name server address: {}", this.brokerConfig.getNamesrvAddr());
			} else if (this.brokerConfig.isFetchNamesrvAddrByAddressServer()) {
				this.scheduledExecutorService.scheduleAtFixedRate(() -> {
					try {
						BrokerController.this.brokerOuterAPI.fetchNameServerAddr();
					} catch (Throwable e) {
						log.error("ScheduledTask fetchNameServerAddr exception", e);
					}
				}, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
			}
			// 如果开启了Dleger做一些相关操作
			if (!messageStoreConfig.isEnableDLegerCommitLog()) {
				if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
					if (this.messageStoreConfig.getHaMasterAddress() != null && this.messageStoreConfig.getHaMasterAddress().length() >= 6) {
						this.messageStore.updateHaMasterAddress(this.messageStoreConfig.getHaMasterAddress());
						this.updateMasterHAServerAddrPeriodically = false;
					} else {
						this.updateMasterHAServerAddrPeriodically = true;
					}
				} else {
					this.scheduledExecutorService.scheduleAtFixedRate(() -> {
						try {
							BrokerController.this.printMasterAndSlaveDiff();
						} catch (Throwable e) {
							log.error("schedule printMasterAndSlaveDiff error.", e);
						}
					}, 1000 * 10, 1000 * 60, TimeUnit.MILLISECONDS);
				}
			}

			if (TlsSystemConfig.tlsMode != TlsMode.DISABLED) {
				// Register a listener to reload SslContext
				try {
					fileWatchService = new FileWatchService(
							new String[]{
									TlsSystemConfig.tlsServerCertPath,
									TlsSystemConfig.tlsServerKeyPath,
									TlsSystemConfig.tlsServerTrustCertPath
							},
							new FileWatchService.Listener() {
								boolean certChanged, keyChanged = false;

								@Override
								public void onChanged(String path) {
									if (path.equals(TlsSystemConfig.tlsServerTrustCertPath)) {
										log.info("The trust certificate changed, reload the ssl context");
										reloadServerSslContext();
									}
									if (path.equals(TlsSystemConfig.tlsServerCertPath)) {
										certChanged = true;
									}
									if (path.equals(TlsSystemConfig.tlsServerKeyPath)) {
										keyChanged = true;
									}
									if (certChanged && keyChanged) {
										log.info("The certificate and private key changed, reload the ssl context");
										certChanged = keyChanged = false;
										reloadServerSslContext();
									}
								}

								private void reloadServerSslContext() {
									((NettyRemotingServer) remotingServer).loadSslContext();
									((NettyRemotingServer) fastRemotingServer).loadSslContext();
								}
							});
				} catch (Exception e) {
					log.warn("FileWatchService created error, can't load the certificate dynamically");
				}
			}
			// 初始化事务相关的一些操作
			initialTransaction();
			// 初始化ACL权限控制相关的东西
			initialAcl();
			// 初始化RPC钩子
			initialRpcHooks();
		}
		return result;
	}

	private void initialTransaction() {
		this.transactionalMessageService = ServiceProvider.loadClass(ServiceProvider.TRANSACTION_SERVICE_ID, TransactionalMessageService.class);
		if (null == this.transactionalMessageService) {
			this.transactionalMessageService = new TransactionalMessageServiceImpl(new TransactionalMessageBridge(this, this.getMessageStore()));
			log.warn("Load default transaction message hook service: {}", TransactionalMessageServiceImpl.class.getSimpleName());
		}
		this.transactionalMessageCheckListener = ServiceProvider.loadClass(ServiceProvider.TRANSACTION_LISTENER_ID, AbstractTransactionalMessageCheckListener.class);
		if (null == this.transactionalMessageCheckListener) {
			this.transactionalMessageCheckListener = new DefaultTransactionalMessageCheckListener();
			log.warn("Load default discard message hook service: {}", DefaultTransactionalMessageCheckListener.class.getSimpleName());
		}
		this.transactionalMessageCheckListener.setBrokerController(this);
		this.transactionalMessageCheckService = new TransactionalMessageCheckService(this);
	}

	private void initialAcl() {
		if (!this.brokerConfig.isAclEnable()) {
			log.info("The broker dose not enable acl");
			return;
		}

		List<AccessValidator> accessValidators = ServiceProvider.load(ServiceProvider.ACL_VALIDATOR_ID, AccessValidator.class);
		if (accessValidators == null || accessValidators.isEmpty()) {
			log.info("The broker dose not load the AccessValidator");
			return;
		}

		for (AccessValidator accessValidator : accessValidators) {
			final AccessValidator validator = accessValidator;
			accessValidatorMap.put(validator.getClass(), validator);
			this.registerServerRPCHook(new RPCHook() {

				@Override
				public void doBeforeRequest(String remoteAddr, RemotingCommand request) {
					//Do not catch the exception
					validator.validate(validator.parse(request, remoteAddr));
				}

				@Override
				public void doAfterResponse(String remoteAddr, RemotingCommand request, RemotingCommand response) {
				}
			});
		}
	}


	private void initialRpcHooks() {

		List<RPCHook> rpcHooks = ServiceProvider.load(ServiceProvider.RPC_HOOK_ID, RPCHook.class);
		if (rpcHooks == null || rpcHooks.isEmpty()) {
			return;
		}
		for (RPCHook rpcHook : rpcHooks) {
			this.registerServerRPCHook(rpcHook);
		}
	}

	public void registerProcessor() {
		/**
		 * SendMessageProcessor
		 */
		SendMessageProcessor sendProcessor = new SendMessageProcessor(this);
		sendProcessor.registerSendMessageHook(sendMessageHookList);
		sendProcessor.registerConsumeMessageHook(consumeMessageHookList);

		this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
		this.remotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
		this.remotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, this.sendMessageExecutor);
		this.remotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE, sendProcessor, this.sendMessageExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.SEND_MESSAGE_V2, sendProcessor, this.sendMessageExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.SEND_BATCH_MESSAGE, sendProcessor, this.sendMessageExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.CONSUMER_SEND_MSG_BACK, sendProcessor, this.sendMessageExecutor);
		/**
		 * PullMessageProcessor
		 */
		this.remotingServer.registerProcessor(RequestCode.PULL_MESSAGE, this.pullMessageProcessor, this.pullMessageExecutor);
		this.pullMessageProcessor.registerConsumeMessageHook(consumeMessageHookList);

		/**
		 * ReplyMessageProcessor
		 */
		ReplyMessageProcessor replyMessageProcessor = new ReplyMessageProcessor(this);
		replyMessageProcessor.registerSendMessageHook(sendMessageHookList);

		this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
		this.remotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE, replyMessageProcessor, replyMessageExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.SEND_REPLY_MESSAGE_V2, replyMessageProcessor, replyMessageExecutor);

		/**
		 * QueryMessageProcessor
		 */
		NettyRequestProcessor queryProcessor = new QueryMessageProcessor(this);
		this.remotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
		this.remotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

		this.fastRemotingServer.registerProcessor(RequestCode.QUERY_MESSAGE, queryProcessor, this.queryMessageExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.VIEW_MESSAGE_BY_ID, queryProcessor, this.queryMessageExecutor);

		/**
		 * ClientManageProcessor
		 */
		ClientManageProcessor clientProcessor = new ClientManageProcessor(this);
		this.remotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.heartbeatExecutor);
		this.remotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
		this.remotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientProcessor, this.clientManageExecutor);

		this.fastRemotingServer.registerProcessor(RequestCode.HEART_BEAT, clientProcessor, this.heartbeatExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.UNREGISTER_CLIENT, clientProcessor, this.clientManageExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.CHECK_CLIENT_CONFIG, clientProcessor, this.clientManageExecutor);

		/**
		 * ConsumerManageProcessor
		 */
		ConsumerManageProcessor consumerManageProcessor = new ConsumerManageProcessor(this);
		this.remotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
		this.remotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
		this.remotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

		this.fastRemotingServer.registerProcessor(RequestCode.GET_CONSUMER_LIST_BY_GROUP, consumerManageProcessor, this.consumerManageExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.UPDATE_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.QUERY_CONSUMER_OFFSET, consumerManageProcessor, this.consumerManageExecutor);

		/**
		 * EndTransactionProcessor
		 */
		this.remotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.endTransactionExecutor);
		this.fastRemotingServer.registerProcessor(RequestCode.END_TRANSACTION, new EndTransactionProcessor(this), this.endTransactionExecutor);

		/**
		 * Default
		 */
		AdminBrokerProcessor adminProcessor = new AdminBrokerProcessor(this);
		this.remotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
		this.fastRemotingServer.registerDefaultProcessor(adminProcessor, this.adminBrokerExecutor);
	}

	public BrokerStats getBrokerStats() {
		return brokerStats;
	}

	public void setBrokerStats(BrokerStats brokerStats) {
		this.brokerStats = brokerStats;
	}

	public void protectBroker() {
		if (this.brokerConfig.isDisableConsumeIfConsumerReadSlowly()) {
			final Iterator<Map.Entry<String, MomentStatsItem>> it = this.brokerStatsManager.getMomentStatsItemSetFallSize().getStatsItemTable().entrySet().iterator();
			while (it.hasNext()) {
				final Map.Entry<String, MomentStatsItem> next = it.next();
				final long fallBehindBytes = next.getValue().getValue().get();
				if (fallBehindBytes > this.brokerConfig.getConsumerFallbehindThreshold()) {
					final String[] split = next.getValue().getStatsKey().split("@");
					final String group = split[2];
					LOG_PROTECTION.info("[PROTECT_BROKER] the consumer[{}] consume slowly, {} bytes, disable it", group, fallBehindBytes);
					this.subscriptionGroupManager.disableConsume(group);
				}
			}
		}
	}

	public long headSlowTimeMills(BlockingQueue<Runnable> q) {
		long slowTimeMills = 0;
		final Runnable peek = q.peek();
		if (peek != null) {
			RequestTask rt = BrokerFastFailure.castRunnable(peek);
			slowTimeMills = rt == null ? 0 : this.messageStore.now() - rt.getCreateTimestamp();
		}

		if (slowTimeMills < 0) {
			slowTimeMills = 0;
		}

		return slowTimeMills;
	}

	public long headSlowTimeMills4SendThreadPoolQueue() {
		return this.headSlowTimeMills(this.sendThreadPoolQueue);
	}

	public long headSlowTimeMills4PullThreadPoolQueue() {
		return this.headSlowTimeMills(this.pullThreadPoolQueue);
	}

	public long headSlowTimeMills4QueryThreadPoolQueue() {
		return this.headSlowTimeMills(this.queryThreadPoolQueue);
	}

	public long headSlowTimeMills4EndTransactionThreadPoolQueue() {
		return this.headSlowTimeMills(this.endTransactionThreadPoolQueue);
	}

	public void printWaterMark() {
		LOG_WATER_MARK.info("[WATERMARK] Send Queue Size: {} SlowTimeMills: {}", this.sendThreadPoolQueue.size(), headSlowTimeMills4SendThreadPoolQueue());
		LOG_WATER_MARK.info("[WATERMARK] Pull Queue Size: {} SlowTimeMills: {}", this.pullThreadPoolQueue.size(), headSlowTimeMills4PullThreadPoolQueue());
		LOG_WATER_MARK.info("[WATERMARK] Query Queue Size: {} SlowTimeMills: {}", this.queryThreadPoolQueue.size(), headSlowTimeMills4QueryThreadPoolQueue());
		LOG_WATER_MARK.info("[WATERMARK] Transaction Queue Size: {} SlowTimeMills: {}", this.endTransactionThreadPoolQueue.size(), headSlowTimeMills4EndTransactionThreadPoolQueue());
	}

	public MessageStore getMessageStore() {
		return messageStore;
	}

	public void setMessageStore(MessageStore messageStore) {
		this.messageStore = messageStore;
	}

	private void printMasterAndSlaveDiff() {
		long diff = this.messageStore.slaveFallBehindMuch();

		// XXX: warn and notify me
		log.info("Slave fall behind master: {} bytes", diff);
	}

	public Broker2Client getBroker2Client() {
		return broker2Client;
	}

	public ConsumerManager getConsumerManager() {
		return consumerManager;
	}

	public ConsumerFilterManager getConsumerFilterManager() {
		return consumerFilterManager;
	}

	public ConsumerOffsetManager getConsumerOffsetManager() {
		return consumerOffsetManager;
	}

	public MessageStoreConfig getMessageStoreConfig() {
		return messageStoreConfig;
	}

	public ProducerManager getProducerManager() {
		return producerManager;
	}

	public void setFastRemotingServer(RemotingServer fastRemotingServer) {
		this.fastRemotingServer = fastRemotingServer;
	}

	public PullMessageProcessor getPullMessageProcessor() {
		return pullMessageProcessor;
	}

	public PullRequestHoldService getPullRequestHoldService() {
		return pullRequestHoldService;
	}

	public SubscriptionGroupManager getSubscriptionGroupManager() {
		return subscriptionGroupManager;
	}

	public void shutdown() {
		if (this.brokerStatsManager != null) {
			this.brokerStatsManager.shutdown();
		}

		if (this.clientHousekeepingService != null) {
			this.clientHousekeepingService.shutdown();
		}

		if (this.pullRequestHoldService != null) {
			this.pullRequestHoldService.shutdown();
		}

		if (this.remotingServer != null) {
			this.remotingServer.shutdown();
		}

		if (this.fastRemotingServer != null) {
			this.fastRemotingServer.shutdown();
		}

		if (this.fileWatchService != null) {
			this.fileWatchService.shutdown();
		}

		if (this.messageStore != null) {
			this.messageStore.shutdown();
		}

		this.scheduledExecutorService.shutdown();
		try {
			this.scheduledExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
		}

		this.unregisterBrokerAll();

		if (this.sendMessageExecutor != null) {
			this.sendMessageExecutor.shutdown();
		}

		if (this.pullMessageExecutor != null) {
			this.pullMessageExecutor.shutdown();
		}

		if (this.replyMessageExecutor != null) {
			this.replyMessageExecutor.shutdown();
		}

		if (this.adminBrokerExecutor != null) {
			this.adminBrokerExecutor.shutdown();
		}

		if (this.brokerOuterAPI != null) {
			this.brokerOuterAPI.shutdown();
		}

		this.consumerOffsetManager.persist();

		if (this.filterServerManager != null) {
			this.filterServerManager.shutdown();
		}

		if (this.brokerFastFailure != null) {
			this.brokerFastFailure.shutdown();
		}

		if (this.consumerFilterManager != null) {
			this.consumerFilterManager.persist();
		}

		if (this.clientManageExecutor != null) {
			this.clientManageExecutor.shutdown();
		}

		if (this.queryMessageExecutor != null) {
			this.queryMessageExecutor.shutdown();
		}

		if (this.consumerManageExecutor != null) {
			this.consumerManageExecutor.shutdown();
		}

		if (this.fileWatchService != null) {
			this.fileWatchService.shutdown();
		}
		if (this.transactionalMessageCheckService != null) {
			this.transactionalMessageCheckService.shutdown(false);
		}

		if (this.endTransactionExecutor != null) {
			this.endTransactionExecutor.shutdown();
		}
	}

	private void unregisterBrokerAll() {
		this.brokerOuterAPI.unregisterBrokerAll(
				this.brokerConfig.getBrokerClusterName(),
				this.getBrokerAddr(),
				this.brokerConfig.getBrokerName(),
				this.brokerConfig.getBrokerId());
	}

	public String getBrokerAddr() {
		return this.brokerConfig.getBrokerIP1() + ":" + this.nettyServerConfig.getListenPort();
	}

	/**
	 * broker启动
	 *
	 * @throws Exception
	 */
	public void start() throws Exception {
		// 启动消息存储组件
		if (this.messageStore != null) {
			this.messageStore.start();
		}
		// 启动netty网络服务器
		if (this.remotingServer != null) {
			this.remotingServer.start();
		}
		if (this.fastRemotingServer != null) {
			this.fastRemotingServer.start();
		}
		// 启动跟文件相关的一个组件
		if (this.fileWatchService != null) {
			this.fileWatchService.start();
		}
		// brokerOuterAPI是十分钟重要的组件 这个组件实际上是broker通过netty客户端发送请求出去给别的组件
		if (this.brokerOuterAPI != null) {
			this.brokerOuterAPI.start();
		}

		if (this.pullRequestHoldService != null) {
			this.pullRequestHoldService.start();
		}

		if (this.clientHousekeepingService != null) {
			this.clientHousekeepingService.start();
		}

		if (this.filterServerManager != null) {
			this.filterServerManager.start();
		}

		if (!messageStoreConfig.isEnableDLegerCommitLog()) {
			startProcessorByHa(messageStoreConfig.getBrokerRole());
			handleSlaveSynchronize(messageStoreConfig.getBrokerRole());
			this.registerBrokerAll(true, false, true);
		}

		/**
		 * 启动broker时启动定时任务 每30s执行一次 向所有namesrv发送心跳包
		 */
		this.scheduledExecutorService.scheduleAtFixedRate(() -> {
			try {
				// 注册自己到namesrv
				BrokerController.this.registerBrokerAll(true, false, brokerConfig.isForceRegister());
			} catch (Throwable e) {
				log.error("registerBrokerAll Exception", e);
			}
		}, 1000 * 10, Math.max(10000, Math.min(brokerConfig.getRegisterNameServerPeriod(), 60000)), TimeUnit.MILLISECONDS);

		if (this.brokerStatsManager != null) {
			this.brokerStatsManager.start();
		}

		if (this.brokerFastFailure != null) {
			this.brokerFastFailure.start();
		}
	}

	public synchronized void registerIncrementBrokerData(TopicConfig topicConfig, DataVersion dataVersion) {
		TopicConfig registerTopicConfig = topicConfig;
		if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
				|| !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
			registerTopicConfig =
					new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
							this.brokerConfig.getBrokerPermission());
		}

		ConcurrentMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
		topicConfigTable.put(topicConfig.getTopicName(), registerTopicConfig);
		TopicConfigSerializeWrapper topicConfigSerializeWrapper = new TopicConfigSerializeWrapper();
		topicConfigSerializeWrapper.setDataVersion(dataVersion);
		topicConfigSerializeWrapper.setTopicConfigTable(topicConfigTable);

		doRegisterBrokerAll(true, false, topicConfigSerializeWrapper);
	}

	/**
	 * broker向namesrv发送心跳包实际调用方法
	 *
	 * @param checkOrderConfig
	 * @param oneway
	 * @param forceRegister
	 */
	public synchronized void registerBrokerAll(final boolean checkOrderConfig, boolean oneway, boolean forceRegister) {

		TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();

		if (!PermName.isWriteable(this.getBrokerConfig().getBrokerPermission())
				|| !PermName.isReadable(this.getBrokerConfig().getBrokerPermission())) {
			ConcurrentHashMap<String, TopicConfig> topicConfigTable = new ConcurrentHashMap<>();
			for (TopicConfig topicConfig : topicConfigWrapper.getTopicConfigTable().values()) {
				TopicConfig tmp =
						new TopicConfig(topicConfig.getTopicName(), topicConfig.getReadQueueNums(), topicConfig.getWriteQueueNums(),
								this.brokerConfig.getBrokerPermission());
				topicConfigTable.put(topicConfig.getTopicName(), tmp);
			}
			topicConfigWrapper.setTopicConfigTable(topicConfigTable);
		}
		// 是否需要注册 如果需要再调用doRegisterBrokerAll真正去注册
		if (forceRegister || needRegister(this.brokerConfig.getBrokerClusterName(),
				this.getBrokerAddr(), this.brokerConfig.getBrokerName(),
				this.brokerConfig.getBrokerId(), this.brokerConfig.getRegisterBrokerTimeoutMills())) {
			doRegisterBrokerAll(checkOrderConfig, oneway, topicConfigWrapper);
		}
	}

	private void doRegisterBrokerAll(boolean checkOrderConfig, boolean oneway, TopicConfigSerializeWrapper topicConfigWrapper) {

		// 调用registerBrokerAll进行注册 将broker注册到所有的namesrv 所以返回结果是List
		List<RegisterBrokerResult> registerBrokerResultList = this.brokerOuterAPI.registerBrokerAll(
				this.brokerConfig.getBrokerClusterName(), this.getBrokerAddr(),
				this.brokerConfig.getBrokerName(), this.brokerConfig.getBrokerId(),
				this.getHAServerAddr(), topicConfigWrapper,
				this.filterServerManager.buildNewFilterServerList(), oneway,
				this.brokerConfig.getRegisterBrokerTimeoutMills(), this.brokerConfig.isCompressedRegister());

		if (registerBrokerResultList.size() > 0) {    // 对注册结果进行处理 涉及MasterHAServer
			RegisterBrokerResult registerBrokerResult = registerBrokerResultList.get(0);
			if (registerBrokerResult != null) {
				if (this.updateMasterHAServerAddrPeriodically && registerBrokerResult.getHaServerAddr() != null) {
					this.messageStore.updateHaMasterAddress(registerBrokerResult.getHaServerAddr());
				}
				this.slaveSynchronize.setMasterAddr(registerBrokerResult.getMasterAddr());
				if (checkOrderConfig) {
					this.getTopicConfigManager().updateOrderTopicConfig(registerBrokerResult.getKvTable());
				}
			}
		}
	}

	private boolean needRegister(final String clusterName,
								 final String brokerAddr,
								 final String brokerName,
								 final long brokerId,
								 final int timeoutMills) {

		TopicConfigSerializeWrapper topicConfigWrapper = this.getTopicConfigManager().buildTopicConfigSerializeWrapper();
		List<Boolean> changeList = brokerOuterAPI.needRegister(clusterName, brokerAddr, brokerName, brokerId, topicConfigWrapper, timeoutMills);
		boolean needRegister = false;
		for (Boolean changed : changeList) {
			if (changed) {
				needRegister = true;
				break;
			}
		}
		return needRegister;
	}

	public TopicConfigManager getTopicConfigManager() {
		return topicConfigManager;
	}

	public void setTopicConfigManager(TopicConfigManager topicConfigManager) {
		this.topicConfigManager = topicConfigManager;
	}

	public String getHAServerAddr() {
		return this.brokerConfig.getBrokerIP2() + ":" + this.messageStoreConfig.getHaListenPort();
	}

	public RebalanceLockManager getRebalanceLockManager() {
		return rebalanceLockManager;
	}

	public SlaveSynchronize getSlaveSynchronize() {
		return slaveSynchronize;
	}

	public ExecutorService getPullMessageExecutor() {
		return pullMessageExecutor;
	}

	public void setPullMessageExecutor(ExecutorService pullMessageExecutor) {
		this.pullMessageExecutor = pullMessageExecutor;
	}

	public BlockingQueue<Runnable> getSendThreadPoolQueue() {
		return sendThreadPoolQueue;
	}

	public FilterServerManager getFilterServerManager() {
		return filterServerManager;
	}

	public BrokerStatsManager getBrokerStatsManager() {
		return brokerStatsManager;
	}

	public List<SendMessageHook> getSendMessageHookList() {
		return sendMessageHookList;
	}

	public void registerSendMessageHook(final SendMessageHook hook) {
		this.sendMessageHookList.add(hook);
		log.info("register SendMessageHook Hook, {}", hook.hookName());
	}

	public List<ConsumeMessageHook> getConsumeMessageHookList() {
		return consumeMessageHookList;
	}

	public void registerConsumeMessageHook(final ConsumeMessageHook hook) {
		this.consumeMessageHookList.add(hook);
		log.info("register ConsumeMessageHook Hook, {}", hook.hookName());
	}

	public void registerServerRPCHook(RPCHook rpcHook) {
		getRemotingServer().registerRPCHook(rpcHook);
		this.fastRemotingServer.registerRPCHook(rpcHook);
	}

	public RemotingServer getRemotingServer() {
		return remotingServer;
	}

	public void setRemotingServer(RemotingServer remotingServer) {
		this.remotingServer = remotingServer;
	}

	public void registerClientRPCHook(RPCHook rpcHook) {
		this.getBrokerOuterAPI().registerRPCHook(rpcHook);
	}

	public BrokerOuterAPI getBrokerOuterAPI() {
		return brokerOuterAPI;
	}

	public InetSocketAddress getStoreHost() {
		return storeHost;
	}

	public void setStoreHost(InetSocketAddress storeHost) {
		this.storeHost = storeHost;
	}

	public Configuration getConfiguration() {
		return this.configuration;
	}

	public BlockingQueue<Runnable> getHeartbeatThreadPoolQueue() {
		return heartbeatThreadPoolQueue;
	}

	public TransactionalMessageCheckService getTransactionalMessageCheckService() {
		return transactionalMessageCheckService;
	}

	public void setTransactionalMessageCheckService(
			TransactionalMessageCheckService transactionalMessageCheckService) {
		this.transactionalMessageCheckService = transactionalMessageCheckService;
	}

	public TransactionalMessageService getTransactionalMessageService() {
		return transactionalMessageService;
	}

	public void setTransactionalMessageService(TransactionalMessageService transactionalMessageService) {
		this.transactionalMessageService = transactionalMessageService;
	}

	public AbstractTransactionalMessageCheckListener getTransactionalMessageCheckListener() {
		return transactionalMessageCheckListener;
	}

	public void setTransactionalMessageCheckListener(
			AbstractTransactionalMessageCheckListener transactionalMessageCheckListener) {
		this.transactionalMessageCheckListener = transactionalMessageCheckListener;
	}


	public BlockingQueue<Runnable> getEndTransactionThreadPoolQueue() {
		return endTransactionThreadPoolQueue;

	}

	public Map<Class, AccessValidator> getAccessValidatorMap() {
		return accessValidatorMap;
	}

	private void handleSlaveSynchronize(BrokerRole role) {
		if (role == BrokerRole.SLAVE) {
			if (null != slaveSyncFuture) {
				slaveSyncFuture.cancel(false);
			}
			this.slaveSynchronize.setMasterAddr(null);
			slaveSyncFuture = this.scheduledExecutorService.scheduleAtFixedRate(() -> {
				try {
					BrokerController.this.slaveSynchronize.syncAll();
				} catch (Throwable e) {
					log.error("ScheduledTask SlaveSynchronize syncAll error.", e);
				}
			}, 1000 * 3, 1000 * 10, TimeUnit.MILLISECONDS);
		} else {
			//handle the slave synchronise
			if (null != slaveSyncFuture) {
				slaveSyncFuture.cancel(false);
			}
			this.slaveSynchronize.setMasterAddr(null);
		}
	}

	public void changeToSlave(int brokerId) {
		log.info("Begin to change to slave brokerName={} brokerId={}", brokerConfig.getBrokerName(), brokerId);

		//change the role
		brokerConfig.setBrokerId(brokerId == 0 ? 1 : brokerId); //TO DO check
		messageStoreConfig.setBrokerRole(BrokerRole.SLAVE);

		//handle the scheduled service
		try {
			this.messageStore.handleScheduleMessageService(BrokerRole.SLAVE);
		} catch (Throwable t) {
			log.error("[MONITOR] handleScheduleMessageService failed when changing to slave", t);
		}

		//handle the transactional service
		try {
			this.shutdownProcessorByHa();
		} catch (Throwable t) {
			log.error("[MONITOR] shutdownProcessorByHa failed when changing to slave", t);
		}

		//handle the slave synchronise
		handleSlaveSynchronize(BrokerRole.SLAVE);

		try {
			this.registerBrokerAll(true, true, brokerConfig.isForceRegister());
		} catch (Throwable ignored) {

		}
		log.info("Finish to change to slave brokerName={} brokerId={}", brokerConfig.getBrokerName(), brokerId);
	}


	public void changeToMaster(BrokerRole role) {
		if (role == BrokerRole.SLAVE) {
			return;
		}
		log.info("Begin to change to master brokerName={}", brokerConfig.getBrokerName());

		//handle the slave synchronise
		handleSlaveSynchronize(role);

		//handle the scheduled service
		try {
			this.messageStore.handleScheduleMessageService(role);
		} catch (Throwable t) {
			log.error("[MONITOR] handleScheduleMessageService failed when changing to master", t);
		}

		//handle the transactional service
		try {
			this.startProcessorByHa(BrokerRole.SYNC_MASTER);
		} catch (Throwable t) {
			log.error("[MONITOR] startProcessorByHa failed when changing to master", t);
		}

		//if the operations above are totally successful, we change to master
		brokerConfig.setBrokerId(0); //TO DO check
		messageStoreConfig.setBrokerRole(role);

		try {
			this.registerBrokerAll(true, true, brokerConfig.isForceRegister());
		} catch (Throwable ignored) {

		}
		log.info("Finish to change to master brokerName={}", brokerConfig.getBrokerName());
	}

	private void startProcessorByHa(BrokerRole role) {
		if (BrokerRole.SLAVE != role) {
			if (this.transactionalMessageCheckService != null) {
				this.transactionalMessageCheckService.start();
			}
		}
	}

	private void shutdownProcessorByHa() {
		if (this.transactionalMessageCheckService != null) {
			this.transactionalMessageCheckService.shutdown(true);
		}
	}


}
