<?php

return [
    //RabbitMQ配置
    'rabbitmq' => [
        'host' => env('SSDK_QUEUE_RABBITMQ_HOST', '127.0.0.1'),
        'port' => env('SSDK_QUEUE_RABBITMQ_PORT', 5672),
        'vhost' => env('SSDK_QUEUE_RABBITMQ_VHOST', '/'),
        'login' => env('SSDK_QUEUE_RABBITMQ_LOGIN', 'guest'),
        'password' => env('SSDK_QUEUE_RABBITMQ_PASSWORD', 'guest'),
        'confirm_timeout' => env('SSDK_QUEUE_RABBITMQ_CONFIRM_TIMEOUT', 1),
    ],

    //网关
    'gateway' => env('SSDK_QUEUE_GATEWAY', ''),

    //日志服务网关
    'log_gateway' => env('SSDK_QUEUE_LOG_GATEWAY', ''),

    //请求超时,默认5s
    'request_timeout' => env('SSDK_QUEUE_REQUEST_TIMEOUT', 5),

    //请求尝试次数,默认2次,不超过3次
    'try_times' => env('SSDK_QUEUE_TRY_TIMES', 2),

    //版本
    'site' => env('SSDK_QUEUE_SITE', 'tester'),

    //场景
    'callerid' => env('SSDK_QUEUE_CALLER_ID', 'tester'),

    //服务标识
    'service_name' => env('SSDK_QUEUE_SERVICE_NAME', 'queue_service'),

    //日志目录
    'log_path' => env('SSDK_QUEUE_LOG_PATH', '/data/nginx_log/job'),

    //日志名称
    'log_name' => env('SSDK_QUEUE_LOG_NAME', 'queue_service'),

    //日志开关，默认打开
    'log_switch' => env('SSDK_QUEUE_LOG_SWITCH', 1),

    //业务异常日志开关，默认打开
    'biz_log_switch' => env('SSDK_QUEUE_BIZ_LOG_SWITCH', 1),

    //Redis配置
    'redis' => [
        'connection' => env('SSDK_QUEUE_REDIS_CONNECTION', 'ssdk_queue'),
        'options' => [
            'host'     => env('SSDK_QUEUE_REDIS_HOST', '127.0.0.1'),
            'port'     => env('SSDK_QUEUE_REDIS_PORT', 6379),
            'database' => env('SSDK_QUEUE_REDIS_DATABASE', 0),
            'password' => env('SSDK_QUEUE_REDIS_PASSWORD', null),
        ],
    ],
];
