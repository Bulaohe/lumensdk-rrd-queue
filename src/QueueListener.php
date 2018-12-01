<?php

namespace Ssdk\Queue;

use Illuminate\Console\Command;
use Swoole\Http\Server;
use Swoole\Process;
use Symfony\Component\Console\Input\InputOption;
use Ssdk\Queue\Services\QueueServiceFacade;

class QueueListener extends Command
{
    /**
     * 控制台命令名称
     *
     * @var string
     */
    protected $signature = 'service:queue:listen {action : start|stop|restart}';

    /**
     * 控制台命令描述
     *
     * @var string
     */
    protected $description = 'Queue Service Listener';

    /**
     * 队列消息处理函数
     *
     * @var callable
     */
    protected $msg_handler = [];

    /**
     * The console command action.
     *
     * @var string
     */
    protected $action;

    /**
     * swoole进程pid
     *
     * @var int
     */
    protected $pid;

    /**
     * pid file
     *
     * @var string
     */
    protected $pid_file;

    /**
     * 创建新的命令实例
     *
     * @return void
     */
    public function __construct()
    {
        parent::__construct();
    }

    /**
     * Configures the current command.
     */
    protected function configure()
    {
        $this->addOption(
            'port',
            'p',
            InputOption::VALUE_REQUIRED,
            'Swoole server port.'
        )->addOption(
            'queue',
            'queue',
            InputOption::VALUE_REQUIRED,
            'Queue name.',
            'default'
        )->addOption(
            'pid_file',
            'pfi',
            InputOption::VALUE_REQUIRED,
            'Swoole server pid file.',
            env('SSDK_QUEUE_LISTENER_PID_FILE', '/tmp/service-queue-listener.pid')
        )->addOption(
            'reactor_num',
            'rn',
            InputOption::VALUE_REQUIRED,
            'Swoole server reactor number.',
            env('SSDK_QUEUE_LISTENER_REACTOR_NUM', 1)
        )->addOption(
            'worker_num',
            'wn',
            InputOption::VALUE_REQUIRED,
            'Swoole server worker number.',
            env('SSDK_QUEUE_LISTENER_WORKER_NUM', 1)
        )->addOption(
            'daemonize',
            'daemon',
            InputOption::VALUE_REQUIRED,
            'Swoole server daemonize.',
            env('SSDK_QUEUE_LISTENER_DAEMONIZE', 1)
        )->addOption(
            'handler_class',
            'hc',
            InputOption::VALUE_REQUIRED,
            'Message handler class',
            env('SSDK_QUEUE_LISTENER_HANDLER_CLASS', '')
        )->addOption(
            'handler_method',
            'hm',
            InputOption::VALUE_REQUIRED,
            'Message handler method',
            env('SSDK_QUEUE_LISTENER_HANDLER_METHOD', '')
        )->addOption(
            'interval',
            'invl',
            InputOption::VALUE_REQUIRED,
            'Message consumption interval',
            env('SSDK_QUEUE_LISTENER_TICK_TIME', 10)
        );
    }

    /**
     * Execute the console command.
     *
     * @return void
     */
    public function handle()
    {
        $this->pid_file = $this->option('pid_file');

        //初始化Message Handler
        $handler_class = $this->option('handler_class');
        $handler_method = $this->option('handler_method');

        $this->msg_handler = [$handler_class, $handler_method];

        $this->initAction();
        $this->runAction();
    }

    /**
     * Run action.
     */
    protected function runAction()
    {
        $this->detectSwoole();

        $this->{$this->action}();
    }

    /**
     * 执行控制台命令
     */
    public function start()
    {
        if ($this->isRunning($this->getPid())) {
            $this->error('Failed! swoole_server process is already running.');
            exit(1);
        }

        $this->info('Starting swoole server...');

        $this->info('> (You can run this command to ensure the ' .
            'swoole_server process is running: ps aux|grep "swoole")');

        $queue = $this->option('queue');
        $server = new Server('127.0.0.1', $this->option('port'));
        $server->set(
            [
                'reactor_num' => $this->option('reactor_num'),
                'worker_num' => $this->option('worker_num'),
                'daemonize' => $this->option('daemonize'),
                'pid_file' => $this->pid_file,
            ]
        );

        //消费频率，默认10ms一次
        $tick_time = $this->option('interval');

        $thisObj = $this;
        $server->on('workerstart', function ($server, $worker_id) use ($queue, $thisObj, $tick_time) {
            //消费消息
            while (true) {
                try {
                    //阻塞
                    QueueServiceFacade::consume(
                        $queue,
                        $thisObj->msg_handler,
                        AMQP_NOPARAM,
                        $tick_time
                    );
                } catch (\Exception $e) {
                    //
                }
                //避免频繁重启消费
                sleep(3);
            }
        });
        $server->on('request', function ($request, $response) {
            return;
        });

        $server->start();
    }

    /**
     * Stop swoole_http_server.
     */
    protected function stop()
    {
        $pid = $this->getPid();

//        if (!$this->isRunning($pid)) {
//            $this->error("Failed! There is no swoole_http_server process running.");
//            exit(1);
//        }

        $this->info('Stopping swoole_http_server...');

        $isRunning = $this->killProcess($pid, SIGTERM, 15);

        if ($isRunning) {
            $this->error('Unable to stop the swoole_http_server process.');
            exit(1);
        }

        // I don't known why Swoole didn't trigger "onShutdown" after sending SIGTERM.
        // So we should manually remove the pid file.
        $this->removePidFile();

        $this->info('> success');
    }

    /**
     * Restart swoole http server.
     */
    protected function restart()
    {
        $pid = $this->getPid();

        if ($this->isRunning($pid)) {
            $this->stop();
        }

        $this->start();
    }

    /**
     * Initialize command action.
     */
    protected function initAction()
    {
        $this->action = $this->argument('action');

        if (!in_array($this->action, ['start', 'stop', 'restart'])) {
            $this->error('Unexpected argument "' . $this->action . '".');
            exit(1);
        }
    }

    /**
     * If Swoole process is running.
     *
     * @param int $pid
     * @return bool
     */
    protected function isRunning($pid)
    {
        if (!$pid) {
            return false;
        }

        Process::kill($pid, 0);

        return !swoole_errno();
    }

    /**
     * Kill process.
     *
     * @param int $pid
     * @param int $sig
     * @param int $wait
     * @return bool
     */
    protected function killProcess($pid, $sig, $wait = 0)
    {
        @Process::kill($pid, $sig);

        if ($wait) {
            $start = time();

            do {
                if (!$this->isRunning($pid)) {
                    break;
                }

                usleep(100000);
            } while (time() < $start + $wait);
        }

        return $this->isRunning($pid);
    }

    /**
     * Get pid.
     *
     * @return int|null
     */
    protected function getPid()
    {
        if ($this->pid) {
            return $this->pid;
        }

        $pid = null;
        $path = $this->getPidPath();

        if (file_exists($path)) {
            $pid = (int)file_get_contents($path);

            if (!$pid) {
                $this->removePidFile();
            } else {
                $this->pid = $pid;
            }
        }

        return $this->pid;
    }

    /**
     * Get Pid file path.
     *
     * @return string
     */
    protected function getPidPath()
    {
        return $this->pid_file;
    }

    /**
     * Remove Pid file.
     */
    protected function removePidFile()
    {
        if (file_exists($this->getPidPath())) {
            unlink($this->getPidPath());
        }
    }

    /**
     * Extension swoole is required.
     */
    protected function detectSwoole()
    {
        if (!extension_loaded('swoole')) {
            $this->error('Extension swoole is required!');

            exit(1);
        }
    }
}
