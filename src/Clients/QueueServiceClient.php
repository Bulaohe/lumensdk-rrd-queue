<?php

namespace Ssdk\Queue\Clients;

use Ssdk\Client\Client as ServiceClient;
use Ssdk\Queue\Log;

/**
 * Class QueueServiceClient
 *
 * {@inheritdoc}
 *
 * 消息队列服务客户端
 *
 * @see http://docs.corp.dodoca.com/pages/viewpage.action?pageId=11087152
 * @package Ssdk\Queue\Clients
 */
class QueueServiceClient
{
    //最大尝试次数
    const MAX_TRY_TIMES = 3;

    //错误码
    const UUID_DUPLICATED = 10009; //uuid冲突

    //网关
    private $gateway;

    //接口入口地址
    private $entrance;

    //请求超时,默认5s
    private $request_timeout;

    //请求尝试次数,不超过3次
    private $try_times;

    //版本
    private $site;

    //场景
    private $callerid;

    //微服务客户端
    private $service_client;

    //服务标识
    private $service_name;

    //日志组件
    private $logger;

    /**
     * Client constructor.
     * @param $config
     */
    public function __construct($config)
    {
        //加载配置
        $this->loadConfig($config);

        //设置微服务客户端
        $this->setServiceClient(app(ServiceClient::class));

        //设置日志组件
        $this->setLogger(app(Log::class));
    }

    /**
     * 获取网关
     *
     * @return mixed
     */
    public function getGateway()
    {
        return $this->gateway;
    }

    /**
     * 设置网关
     *
     * @param mixed $gateway
     * @return $this
     */
    public function setGateway($gateway)
    {
        $this->gateway = $gateway;
        return $this;
    }

    /**
     * 获取请求超时
     *
     * @return mixed
     */
    public function getRequestTimeout()
    {
        return $this->request_timeout;
    }

    /**
     * 设置请求超时
     *
     * @param mixed $request_timeout
     * @return $this
     */
    public function setRequestTimeout($request_timeout)
    {
        $this->request_timeout = $request_timeout;
        return $this;
    }

    /**
     * 获取请求尝试次数
     *
     * @return mixed
     */
    public function getTryTimes()
    {
        return $this->try_times;
    }

    /**
     * 设置请求尝试次数
     *
     * @param mixed $try_times
     * @return $this
     */
    public function setTryTimes($try_times)
    {
        $this->try_times = $try_times;
        return $this;
    }

    /**
     * 获取版本
     *
     * @return mixed
     */
    public function getSite()
    {
        return $this->site;
    }

    /**
     * 设置版本
     *
     * @param mixed $site
     * @return $this
     */
    public function setSite($site)
    {
        $this->site = $site;
        return $this;
    }

    /**
     * 获取场景
     *
     * @return mixed
     */
    public function getCallerid()
    {
        return $this->callerid;
    }

    /**
     * 设置场景
     *
     * @param mixed $callerid
     * @return $this
     */
    public function setCallerid($callerid)
    {
        $this->callerid = $callerid;
        return $this;
    }

    /**
     * 获取微服务客户端
     *
     * @return mixed
     */
    public function getServiceClient()
    {
        return $this->service_client;
    }

    /**
     * 设置微服务客户端
     *
     * @param mixed $service_client
     * @return $this
     */
    public function setServiceClient($service_client)
    {
        $this->service_client = $service_client;
        return $this;
    }

    /**
     * 获取服务标识
     *
     * @return mixed
     */
    public function getServiceName()
    {
        return $this->service_name;
    }

    /**
     * 设置服务标识
     *
     * @param mixed $service_name
     * @return $this
     */
    public function setServiceName($service_name)
    {
        $this->service_name = $service_name;
        return $this;
    }

    /**
     * 获取日志组件
     *
     * @return mixed
     */
    public function getLogger()
    {
        return $this->logger;
    }

    /**
     * 设置日志组件
     *
     * @param mixed $logger
     * @return $this
     */
    public function setLogger($logger)
    {
        $this->logger = $logger;
        return $this;
    }

    /**
     * 加载配置
     *
     * @param $config 配置
     */
    private function loadConfig($config)
    {
        $this->setGateway($config['gateway']);
        $this->setRequestTimeout($config['request_timeout']);
        $this->setTryTimes($config['try_times'] > self::MAX_TRY_TIMES ?
            self::MAX_TRY_TIMES : $config['try_times']);
        $this->setSite($config['site']);
        $this->setCallerid($config['callerid']);
        $this->setServiceName($config['service_name']);
    }

    /**
     * 发起请求
     *
     * @param string $api_uri 接口地址
     * @param array $params 请求参数
     * @param string $gateway 指定网关地址
     * @return array|mixed
     */
    public function performRequest($api_uri, array $params, $gateway = '')
    {
        //格式化接口请求参数
        $form_params = array_merge(['data' => $params], $this->getPublicRequestParams());

        //设置服务标识
        $this->getServiceClient()->setServiceName($this->getServiceName());
        //设置网关地址
        $this->getServiceClient()->setGateway($gateway ? : $this->getGateway());
        //设置请求尝试次数
        $this->getServiceClient()->setTryTimes($this->getTryTimes());

        $data = null;

        //发起HTTP请求
        for ($i = 0; $i < $this->getTryTimes(); ++$i) {
            //记录请求日志
            $this->getLogger()->write(array_merge($form_params, ['action' => 'request']));
            $response_body = $this->getServiceClient()->performRequest('POST', $api_uri, [
                'form_params' => $form_params,
                'timeout' => $this->getRequestTimeout(),
                'read_timeout' => $this->getRequestTimeout(),
                'connect_timeout' => $this->getRequestTimeout(),
            ]);
            $data = json_decode($response_body, true);
            //记录响应日志
            $this->getLogger()->write(array_merge(
                is_array($data) ? $data : ['response_body' => $response_body],
                ['action' => 'response']
            ));
            if (!json_last_error()) {
                //接口返回uuid冲突，更换uuid重试
                if (!empty($data['status']) && $data['status'] == self::UUID_DUPLICATED) {
                    $form_params['uuid'] = $this->generateUuid();
                    continue;
                }
            }

            break;
        }

        return $data;
    }

    /**
     * 获取公共请求参数
     *
     * @return array
     */
    private function getPublicRequestParams()
    {
        return [
            'site' => $this->getSite(), //版本
            'callerid' => $this->getCallerid(), //场景
            'uuid' => $this->generateUuid(), //请求uuid标识
        ];
    }

    /**
     * 生成uuid，不保证全局唯一
     *
     * @return string
     */
    private function generateUuid()
    {
        return $this->getCallerid() . md5(uniqid(mt_rand()) . microtime());
    }
}
