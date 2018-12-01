<?php

namespace Ssdk\Queue\Clients;

/**
 * Class RabbitMQClient
 *
 * {@inheritdoc}
 *
 * RabbitMQ消息队列客户端
 *
 * @package Ssdk\Queue\Clients
 */
class RabbitMQClient
{
    const AMQP_EX_TYPE_DELAY = 'x-delayed-message';

    /**
     * @var \AMQPConnection
     */
    protected $connection;

    /**
     * @var \AMQPChannel
     */
    protected $channel;

    /**
     * @var \AMQPExchange[]
     */
    protected $exchanges = [];

    /**
     * @var \AMQPQueue[]
     */
    protected $queues = [];

    protected $host;

    protected $port;

    protected $vhost;

    protected $login;

    protected $password;

    protected $confirm_timeout = 1;

    /**
     * RabbitMQClient constructor.
     * @param $config
     */
    public function __construct($config)
    {
        //初始化配置
        $this->initConfig($config);
    }

    /**
     * 创建exchange实例
     *
     * @param $name
     * @param string $type AMQP_EX_TYPE_DIRECT|AMQP_EX_TYPE_FANOUT|AMQP_EX_TYPE_HEADERS|AMQP_EX_TYPE_TOPIC
     * @param array $args
     * @param bool $replace
     * @param int $flags
     * @return $this
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function createExchange(
        $name,
        $type = AMQP_EX_TYPE_DIRECT,
        $args = [],
        $replace = false,
        $flags = AMQP_DURABLE
    )
    {
        if (!$replace && isset($this->exchanges[$name])) {
            return $this;
        }

        $exchange = new \AMQPExchange($this->getChannel());
        $exchange->setName($name);
        $exchange->setType($type);
        $exchange->setFlags($flags);
        $exchange->setArguments($args);

        $this->exchanges[$name] = $exchange;

        return $this;
    }

    /**
     * 创建queue实例
     *
     * @param $name
     * @param int $flags AMQP_DURABLE|AMQP_PASSIVE|AMQP_EXCLUSIVE|AMQP_AUTODELETE|AMQP_NOPARAM
     * @param bool $replace
     * @param bool $is_delay
     * @param string $normal_exchange_name
     * @param array $args
     * @return $this
     * @throws \AMQPConnectionException
     * @throws \AMQPQueueException
     */
    public function createQueue(
        $name,
        $flags = AMQP_DURABLE,
        $replace = false,
        $is_delay = false,
        $normal_exchange_name = '',
        $args = []
    )
    {
        if (!$replace && isset($this->queues[$name])) {
            return $this;
        }

        if ($is_delay) {
            $args['x-dead-letter-exchange'] = $normal_exchange_name;
        }

        $queue = new \AMQPQueue($this->getChannel());
        $queue->setName($name);
        $queue->setFlags($flags);
        $queue->setArguments($args);

        $this->queues[$name] = $queue;

        return $this;
    }

    /**
     * 绑定queue到exchange
     *
     * @param $exchange_name
     * @param $queue_name
     * @param null $routing_key
     * @param array $arguments
     * @return bool
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     */
    public function bindQueue($exchange_name, $queue_name, $routing_key = null, array $arguments = array())
    {
        if (isset($this->exchanges[$exchange_name])) {
            $exchange = $this->exchanges[$exchange_name];

            if (isset($this->queues[$queue_name])) {
                $queue = $this->queues[$queue_name];

                return $queue->bind($exchange->getName(), $routing_key, $arguments);
            }
        }

        return false;
    }

    /**
     * 声明exchange
     *
     * @param $exchange_name
     * @return bool
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function declareExchange($exchange_name)
    {
        if (isset($this->exchanges[$exchange_name])) {
            $exchange = $this->exchanges[$exchange_name];
            return $exchange->declareExchange();
        }

        return false;
    }

    /**
     * 声明queue
     *
     * @param $queue_name
     * @return bool|int
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     */
    public function declareQueue($queue_name)
    {
        if (isset($this->queues[$queue_name])) {
            $queue = $this->queues[$queue_name];
            return $queue->declareQueue();
        }

        return false;
    }

    /**
     * 生产消息
     *
     * @param $exchange_name
     * @param $message
     * @param null $routing_key
     * @param int $flags AMQP_MANDATORY|AMQP_IMMEDIATE|AMQP_NOPARAM
     * @param array $attributes
     * @param bool $is_confirm
     * @return bool
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPExchangeException
     */
    public function produce(
        $exchange_name,
        $message,
        $routing_key = null,
        $flags = AMQP_NOPARAM,
        array $attributes = array(),
        $is_confirm = true
    ) {
        if (isset($this->exchanges[$exchange_name])) {
            $exchange = $this->exchanges[$exchange_name];
            $result = false;

            //生产确认
            if ($is_confirm) {
                $channel = $this->getChannel();
                $channel->confirmSelect();

                //设置确认回调
                $channel->setConfirmCallback(function (int $delivery_tag, bool $multiple) use (&$result) {
                    $result = true;
                    return false;
                }, function (int $delivery_tag, bool $multiple, bool $requeue) use (&$result) {
                    $result = false;
                    return false;
                });

                $exchange->publish($message, $routing_key, $flags, $attributes);

                //同步等待回调确认
                $channel->waitForConfirm($this->confirm_timeout);
            } else {
                $result = $exchange->publish($message, $routing_key, $flags, $attributes);
            }

            return $result;
        }

        return false;
    }

    /**
     * 同步消费消息
     *
     * @param $queue_name
     * @param int $flags AMQP_AUTOACK|AMQP_NOPARAM
     * @return \AMQPEnvelope|bool
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     */
    public function syncConsume($queue_name, $flags = AMQP_NOPARAM)
    {
        if (isset($this->queues[$queue_name])) {
            $queue = $this->queues[$queue_name];
            return $queue->get($flags);
        }

        return false;
    }

    /**
     * 异步消费消息
     *
     * @param $queue_name
     * @param callable $callback
     * @param int $flags
     * @param string $consumer_tag
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     * @throws \AMQPEnvelopeException
     */
    public function asyncConsume($queue_name, callable $callback, $flags = AMQP_NOPARAM, $consumer_tag = null)
    {
        if (isset($this->queues[$queue_name])) {
            $queue = $this->queues[$queue_name];
            $queue->consume($callback, $flags, $consumer_tag);
        }
    }

    /**
     * 取消异步消费
     *
     * @param $queue_name
     * @param $consumer_tag
     * @throws \AMQPChannelException
     * @throws \AMQPConnectionException
     */
    public function cancelAsyncConsumer($queue_name, $consumer_tag)
    {
        if (isset($this->queues[$queue_name])) {
            $queue = $this->queues[$queue_name];
            $queue->cancel($consumer_tag);
        }
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
     */
    public function ack($queue_name, $delivery_tag, $flags = AMQP_NOPARAM)
    {
        if (isset($this->queues[$queue_name])) {
            $queue = $this->queues[$queue_name];
            return $queue->ack($delivery_tag, $flags);
        }

        return false;
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
     */
    public function reject($queue_name, $delivery_tag, $flags = AMQP_NOPARAM)
    {
        if (isset($this->queues[$queue_name])) {
            $queue = $this->queues[$queue_name];
            return $queue->reject($delivery_tag, $flags);
        }

        return false;
    }

    /**
     * 初始化配置
     *
     * @param $config
     */
    protected function initConfig($config)
    {
        $this->host = $config['host'];
        $this->port = $config['port'];
        $this->vhost = $config['vhost'];
        $this->login = $config['login'];
        $this->password = $config['password'];
        $this->confirm_timeout = $config['confirm_timeout'];
    }

    /**
     * 建立tcp连接
     *
     * @throws \AMQPConnectionException
     */
    protected function connect()
    {
        $this->connection = new \AMQPConnection([
            'host' => $this->host,
            'port' => $this->port,
            'vhost' => $this->vhost,
            'login' => $this->login,
            'password' => $this->password
        ]);
        $this->connection->pconnect();
    }

    /**
     * 重新建立tcp连接
     *
     * @throws \AMQPConnectionException
     */
    protected function reconnect()
    {
        //连接不存在，建立连接
        if (!$this->connection) {
            $this->connect();
            $this->createChannel();
            $this->clearQueues();
            $this->clearExchanges();
        }

        //连接断开，重新连接
        if (!$this->connection->isConnected()) {
            $this->connection->preconnect();
            $this->createChannel();
            $this->clearQueues();
            $this->clearExchanges();
        }
    }

    /**
     * 重新创建channel实例
     *
     * @throws \AMQPConnectionException
     */
    protected function recreateChannel()
    {
        //channel不存在，创建
        if (!$this->channel) {
            $this->createChannel();
            $this->clearQueues();
            $this->clearExchanges();
        }

        //channel断开，重新创建
        if (!$this->channel->isConnected()) {
            $this->createChannel();
            $this->clearQueues();
            $this->clearExchanges();
        }
    }

    /**
     * 清除queue对象
     */
    protected function clearQueues()
    {
        $this->queues = [];
    }

    /**
     * 清除exchange对象
     */
    protected function clearExchanges()
    {
        $this->exchanges = [];
    }

    /**
     * 检查连接存活
     *
     * @throws \AMQPConnectionException
     */
    public function checkAlive()
    {
        //尝试重连，方法内判断连接存活
        $this->reconnect();
        $this->recreateChannel();
        return $this;
    }

    /**
     * 获取tcp连接
     *
     * @return \AMQPConnection
     * @throws \AMQPConnectionException
     */
    protected function getConnection()
    {
        if (!$this->connection) {
            $this->connect();
        }
        return $this->connection;
    }

    /**
     * 创建channel实例
     *
     * @throws \AMQPConnectionException
     */
    protected function createChannel()
    {
        $this->channel = new \AMQPChannel($this->getConnection());
    }

    /**
     * 获取channel实例
     *
     * @return \AMQPChannel
     * @throws \AMQPConnectionException
     */
    protected function getChannel()
    {
        if (!$this->channel) {
            $this->createChannel();
        }
        return $this->channel;
    }
}
