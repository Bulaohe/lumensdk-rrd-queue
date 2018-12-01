<?php

namespace Ssdk\Queue;

use Illuminate\Console\Command;
use Swoole\Http\Server;
use Swoole\Process;
use Symfony\Component\Console\Input\InputOption;
use Ssdk\Queue\Services\QueueServiceFacade;

class QueueGroupListener extends Command
{
    /**
     * 控制台命令名称
     *
     * @var string
     */
    protected $signature = 'service:queue-group:listen {action : start|stop|restart}';

    /**
     * 控制台命令描述
     *
     * @var string
     */
    protected $description = 'Queue Group Service Listener';

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
     * group array file path
     *
     * @var string
     */
    protected $group_arr;
    
    /**
     * return from $group_arr file
     * 
     * @var array
     */
    protected $groups;

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
            'group_arr',
            'group_arr',
            InputOption::VALUE_REQUIRED,
            'Queue Group Array File Path.',
            'noooooooooffile'
        )->addOption(
            'pid_file',
            'pfi',
            InputOption::VALUE_REQUIRED,
            'Swoole server pid file.',
            env('SSDK_QUEUE_LISTENER_PID_FILE', '/tmp/service-queue-group-listener.pid')
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
        $this->group_arr = $this->option('group_arr');

        //初始化Message Handler
        
        if($this->group_arr == 'noooooooooffile'){
            $this->info('no group file...');
            exit;
        }else{
            if(strpos($this->group_arr, '.php') !== false && file_exists($this->group_arr)){
                $this->groups = require $this->group_arr;
                if(!isset($this->groups['groups'])){
                    $this->info('invalid group data...');
                    exit;
                }
            }else{
                $this->info('wrong group file...');
                exit;
            }
        }

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

        $server = new Server('127.0.0.1', $this->option('port'),SWOOLE_PROCESS,SWOOLE_SOCK_TCP);
        $groups = $this->groups['groups'];
        
        $task_num = count($groups);
        $server->set(
            [
                'reactor_num' => $this->option('reactor_num'),
                'worker_num' => $this->option('worker_num'),
                'daemonize' => $this->option('daemonize'),
                'pid_file' => $this->pid_file,
                'task_worker_num' => $task_num,
            ]
        );

        //消费频率，默认10ms一次
        $tick_time = $this->option('interval');

        $thisObj = $this;
        $server->on('workerstart', function ($server, $worker_id) use ($groups, $thisObj, $tick_time) {
            
            if($server->taskworker === false){
                //消费消息
                foreach ($groups as $group){
                    
                    $server->task($group);
                }
            }
        });
        $server->on('request', function ($request, $response) {
            return;
        });
        
            $server->on('task', function ($server, $task_id, $reactor_id, $data)  use ($thisObj, $tick_time) {
            echo "New AsyncTask[id=$task_id]\n";
            
            while (true) {
                try {
                    QueueServiceFacade::consume(
                        $data['queue_name'],
                        [$data['handle_class'], $data['handle_method']],
                        AMQP_NOPARAM,
                        $tick_time
                        );
                    
                } catch (\Exception $e) {
                    //
                    var_dump($e->getTraceAsString());
                }
                //避免频繁重启消费
                sleep(3);
            }
            
            $server->finish("$data -> OK");
        });

        $server->on('finish', function ($server, $task_id, $data) {
            echo "AsyncTask[$task_id] finished: {$data}\n";
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
