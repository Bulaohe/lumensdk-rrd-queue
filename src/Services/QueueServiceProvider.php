<?php

namespace Ssdk\Queue\Services;

use Illuminate\Support\ServiceProvider;
use Ssdk\Queue\Clients\QueueServiceClient;
use Ssdk\Queue\Clients\RabbitMQClient;
use Ssdk\Queue\Log;
use Ssdk\Queue\QueueListener;
use Ssdk\Queue\QueueGroupListener;

class QueueServiceProvider extends ServiceProvider
{
    /**
     * 资源依赖注入
     */
    public function register()
    {
        $config = config('ssdk_queue');

        //注入Client
        $this->app->singleton(QueueServiceClient::class, function() use ($config) {
            return new QueueServiceClient($config);
        });

        //注入日志组件
        $this->app->singleton(Log::class, function () use ($config) {
            return new Log($config);
        });

        //注入队列服务
        $this->app->singleton(QueueService::class, function() use ($config) {
            return new RrdQueueService($config);
        });

        //注入RabbitMQ客户端
        $this->app->singleton(RabbitMQClient::class, function() use ($config) {
            return new RabbitMQClient($config['rabbitmq']);
        });

        //注册队列监听器
        $this->commands([
            QueueListener::class,
            QueueGroupListener::class,
        ]);

        //注入redis配置
        config(['database.redis.' . $config['redis']['connection'] => $config['redis']['options']]);
    }

    /**
     * APP启动
     */
    public function boot()
    {
        //发布配置文件
        $this->publishes([
            __DIR__ . '/config/ssdk_queue.php' => base_path('config/ssdk_queue.php'),
        ]);
    }
}
