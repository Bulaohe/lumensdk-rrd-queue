<?php

namespace Ssdk\Queue\Services;

use Illuminate\Support\Facades\Redis;
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
class RrdQueueService extends QueueService
{
    /**
     * 消费消息
     *
     * @param $queue_name
     * @param array $msg_handler 业务回调函数
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
        $msg_handler = ['', ''],
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

                    $msg_job = null;

                    $msg_params = json_decode($msg, true);

                    if (class_exists($msg_handler[0])) {
                        $handler_object = app($msg_handler[0], $msg_params);
                        if (method_exists($handler_object, $msg_handler[1])) {
                            $msg_job = [$handler_object, $msg_handler[1]];
                        }
                    }

                    if (!is_callable($msg_job)) {
                        if (class_exists($handler_class)) {
                            $handler_object = app($handler_class, $msg_params);
                            if (method_exists($handler_object, $handler_method)) {
                                $msg_job = [$handler_object, $handler_method];
                            }
                        }
                    }

                    //业务回调函数返回false或者捕获异常时消费失败，等待再次消费
                    $handle_result = false;
                    if (is_callable($msg_job)) {
                        try {
                            $handle_result = app()->call($msg_job);
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
}
