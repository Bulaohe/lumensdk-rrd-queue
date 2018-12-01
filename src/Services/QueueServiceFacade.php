<?php

namespace Ssdk\Queue\Services;

use Illuminate\Support\Facades\Facade;

class QueueServiceFacade extends Facade
{
    protected static function getFacadeAccessor()
    {
        return QueueService::class;
    }
}
