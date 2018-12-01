<?php

namespace Ssdk\Queue\Services;

use Illuminate\Support\Facades\Redis;
use Ssdk\Queue\Clients\QueueServiceClient;
use Ssdk\Queue\Clients\RabbitMQClient;
use Ssdk\Queue\Log;

/**
 * Class QueueService
 *
 * {@inheritdoc}
 *
 * 消息队列服务
 *
 * @package Ssdk\Queue\Services
 */
class QueueService
{
    //生产队列消息接口地址
    const PRODUCE_API_URI = '/queue/produce';

    //记录消息日志接口地址
    const LOG_API_URI = '/queue/log';

    //消息状态
    const MSG_RECEIVE = 0; //消息接收
    const MSG_ENQ_SUCCESS = 1; //入队成功
    const MSG_ENQ_FAILED = 2; //入队失败
    const MSG_DEQ_SUCCESS = 3; //出队成功
    const MSG_ACK = 4; //消费成功
    const MSG_REQUEUE = 5; //消费失败重试
    const MSG_FAILED = 6; //最终消费失败

    /**
     * @var RabbitMQClient
     */
    protected $rabbitMQClient;

    /** @var QueueServiceClient */
    protected $queueServiceClient;

    /** @var Log */
    protected $logger;

    /** @var \Redis */
    protected $redis;

    /** @var array */
    protected $config;

    /**
     * QueueService constructor.
     * @param $config
     */
    public function __construct($config)
    {
        $this->config = $config;
    }

    /**
     * 获取RabbitMQ客户端
     *
     * @return RabbitMQClient
     * @throws \AMQPConnectionException
     */
    protected function getRabbitMQClient()
    {
        if (!$this->rabbitMQClient) {
            //没有客户端，初始化客户端
            $this->rabbitMQClient = app(RabbitMQClient::class);
        } else {
            //客户端存在，检查连接存活
            $this->rabbitMQClient->checkAlive();
        }

        return $this->rabbitMQClient;
    }

    /**
     * 获取队列服务客户端
     */
    protected function getQueueServiceClient()
    {
        if (!$this->queueServiceClient) {
            //注入队列服务客户端
            $this->queueServiceClient = app(QueueServiceClient::class);
        }

        return $this->queueServiceClient;
    }

    /**
     * 获取日志组件
     *
     * @return Log
     */
    protected function getLogger()
    {
        if (!$this->logger) {
            $this->logger = app(Log::class);
        }

        return $this->logger;
    }

    /**
     * 获取redis实例
     *
     * @return \Redis
     */
    protected function getRedis()
    {
        if (!$this->redis) {
            //注入redis实例
            $this->redis = Redis::connection($this->config['redis']['connection']);
        }

        return $this->redis;
    }

    /**
     * 生产消息
     *
     * @param $queue_name
     * @param $message
     * @param string $biz_type
     * @param int $delay
     * @param string $handler_class
     * @param string $handler_method
     * @param int $max_retry_times 最大重新消费次数，默认1次，最多5次
     * @return string
     */
    public function produce(
        $queue_name,
        $message,
        $biz_type = '',
        $delay = 0,
        $handler_class = '',
        $handler_method = '',
        $max_retry_times = 1
    )
    {
        //必传参数
        $params = [
            'queue_name' => $queue_name,
            'message' => $message,
            'delay' => $delay,
            'max_retry_times' => $max_retry_times
        ];

        //可选参数
        if ($biz_type) {
            $params['biz_type'] = $biz_type;
        }
        if ($handler_class) {
            $params['handler_class'] = $handler_class;
        }
        if ($handler_method) {
            $params['handler_method'] = $handler_method;
        }

        $response = $this->getQueueServiceClient()->performRequest(self::PRODUCE_API_URI, $params);

        if (isset($response['status'])) {
            if ($response['status'] == 0) {
                return $response['result']['msg_id'];
            }
        }

        return '';
    }

    /**
     * 记录消息日志
     *
     * @param $msg_id
     * @param $status
     * @param $queue_name
     * @param $message
     * @param string $biz_type
     * @param int $delay
     * @param $handler_class
     * @param $handler_method
     * @param int $max_retry_times
     * @param int $retry_times
     */
    public function log(
        $msg_id,
        $status,
        $queue_name,
        $message,
        $biz_type = '',
        $delay = 0,
        $handler_class = '',
        $handler_method = '',
        $max_retry_times = 1,
        $retry_times = 0
    )
    {
        //必传参数
        $message_log = [
            'msg_id' => $msg_id,
            'status' => $status,
            'queue_name' => $queue_name,
            'message' => $message,
            'delay' => $delay,
            'max_retry_times' => $max_retry_times,
            'retry_times' => $retry_times
        ];

        //可选参数
        if ($biz_type) {
            $message_log['biz_type'] = $biz_type;
        }
        if ($handler_class) {
            $message_log['handler_class'] = $handler_class;
        }
        if ($handler_method) {
            $message_log['handler_method'] = $handler_method;
        }

        $this->getQueueServiceClient()->performRequest(
            self::LOG_API_URI,
            $message_log,
            isset($this->config['log_gateway']) ? $this->config['log_gateway'] : ''
        );
    }

    /**
     * 取消消费消息
     *
     * @param $queue_name
     * @param $consumer_tag
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function cancelConsume($queue_name, $consumer_tag)
    {
        $this->getRabbitMQClient()->createQueue($queue_name)
            ->cancelAsyncConsumer($queue_name, $consumer_tag);
    }

    /**
     * 消费消息
     *
     * @param $queue_name
     * @param callable $msg_handler 业务回调函数
     * @param int $flags AMQP_NOPARAM|AMQP_AUTOACK
     * @param int $interval 消费频率，默认10ms一次
     * @param null $consumer_tag
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPEnvelopeException
     * @throws \AMQPQueueException
     */
    public function consume(
        $queue_name,
        $msg_handler = null,
        $flags = AMQP_NOPARAM,
        $interval = 10,
        $consumer_tag = null
    )
    {
        $thisObj = $this;

        $this->getRabbitMQClient()->createQueue($queue_name)
            ->asyncConsume(
                $queue_name,
                function (\AMQPEnvelope $envelope, \AMQPQueue $q) use ($msg_handler, $thisObj, $flags, $interval) {
                    $headers = $envelope->getHeaders();
                    $msg = $envelope->getBody();
                    $msg_id = $headers['x-msg-id'];
                    $delay = isset($headers['x-delay']) ? intval($headers['x-delay']) : 0;
                    $biz_type = isset($headers['x-biz-type']) ? $headers['x-biz-type'] : '';
                    $handler_class = isset($headers['x-handler-class']) ? $headers['x-handler-class'] : '';
                    $handler_method = isset($headers['x-handler-method']) ? $headers['x-handler-method'] : '';
                    $max_retry_times = isset($headers['x-max-retry-times']) ?
                        intval($headers['x-max-retry-times']) : 1;
                    $queue_name = $q->getName();

                    $retry_times = intval($thisObj->getRedis()->get('service:queue:msg:requeue:times:' . $msg_id));

                    //消息出队日志
                    $thisObj->log(
                        $msg_id,
                        self::MSG_DEQ_SUCCESS,
                        $queue_name,
                        $msg,
                        $biz_type,
                        $delay,
                        $handler_class,
                        $handler_method,
                        $max_retry_times,
                        $retry_times
                    );

                    //决定handler，参数传入的优先
                    if (!is_callable($msg_handler)) {
                        if (class_exists($handler_class)) {
                            $handler_object = app($handler_class);
                            if (method_exists($handler_object, $handler_method)) {
                                $msg_handler = [$handler_object, $handler_method];
                            }
                        }
                    }

                    //业务回调函数返回false或者捕获异常时消费失败，等待再次消费
                    $handle_result = false;
                    if (is_callable($msg_handler)) {
                        try {
                            $handle_result = call_user_func_array($msg_handler, ['msg' => $msg]);
                        } catch (\Exception $e) {
                            $handle_result = false;

                            if ($this->config['biz_log_switch']) {
                                $this->getLogger()->write([
                                    'message' => $e->getMessage(),
                                    'trace' => $e->getTraceAsString()
                                ], true, 'consume', $queue_name);
                            }
                        }
                    }

                    if ($handle_result === false) {
                        //判断重试次数上限
                        $reject_flags = $retry_times < $max_retry_times ? AMQP_REQUEUE : AMQP_NOPARAM;

                        //维护消息重试次数
                        if ($reject_flags == AMQP_REQUEUE) { //累计重试次数
                            $thisObj->getRedis()->incr('service:queue:msg:requeue:times:' . $msg_id);
                        } else { //清空重试次数
                            $thisObj->getRedis()->del('service:queue:msg:requeue:times:' . $msg_id);
                        }

                        //消息状态
                        $message_status = $reject_flags == AMQP_REQUEUE ? self::MSG_REQUEUE : self::MSG_FAILED;

                        //拒绝消费消息，等待再次消费，存在重复执行业务逻辑的风险
                        try {
                            $flags == AMQP_AUTOACK || $q->reject($envelope->getDeliveryTag(), $reject_flags);
                        } catch (\Exception $e) {
                            throw $e;
                        } finally {
                            //消息消费失败日志
                            $thisObj->log(
                                $msg_id,
                                $message_status,
                                $queue_name,
                                $msg,
                                $biz_type,
                                $delay,
                                $handler_class,
                                $handler_method,
                                $max_retry_times,
                                $retry_times
                            );
                        }
                    } else {
                        //确认消费消息
                        try {
                            $flags == AMQP_AUTOACK || $q->ack($envelope->getDeliveryTag());

                            //清空重试次数
                            $thisObj->getRedis()->del('service:queue:msg:requeue:times:' . $msg_id);
                        } catch (\Exception $e) {
                            throw $e;
                        } finally {
                            //消息消费成功日志
                            $thisObj->log(
                                $msg_id,
                                self::MSG_ACK,
                                $queue_name,
                                $msg,
                                $biz_type,
                                $delay,
                                $handler_class,
                                $handler_method,
                                $max_retry_times,
                                $retry_times
                            );
                        }
                    }

                    //控制消费频率
                    usleep($interval * 1000);
                },
                $flags, $consumer_tag
            );
    }

    /**
     * 消费成功
     *
     * @param $queue_name
     * @param $delivery_tag
     * @param int $flags AMQP_MULTIPLE|AMQP_NOPARAM
     * @return bool
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function ack($queue_name, $delivery_tag, $flags = AMQP_NOPARAM)
    {
        return $this->getRabbitMQClient()->createQueue($queue_name)
            ->ack($queue_name, $delivery_tag, $flags);
    }

    /**
     * 消费失败
     *
     * @param $queue_name
     * @param $delivery_tag
     * @param int $flags AMQP_REQUEUE|AMQP_NOPARAM
     * @return bool
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function reject($queue_name, $delivery_tag, $flags = AMQP_REQUEUE)
    {
        return $this->getRabbitMQClient()->createQueue($queue_name)
            ->reject($queue_name, $delivery_tag, $flags);
    }
}
