<?php
return [
    'groups' => [
        [
            'queue_name'=>'order_pastdue',
            'handle_class'=>'\\App\\Jobs\\OrderPastDue',
            'handle_method'=>'handle',
        ],
    ]
];