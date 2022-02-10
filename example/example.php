#!/usr/bin/env php
<?php

require "vendor/autoload.php";

$sdk = new \Aws\Sdk();
$streamName = getenv('STREAM_NAME');
$checkpoints = [];
$kinesisClient = $sdk->createKinesis([
    'version' => 'latest',
    'region' => getenv('AWS_REGION'),
    'credentials' => [
        'key' => getenv('AWS_KEY'),
        'secret' => getenv('AWS_SECRET'),
    ],
]);

kinsume(
    kinesisClient: $kinesisClient,
    streamName: $streamName,
    shardIteratorBuilder: function ($shardId) use (&$checkpoints) {
        if (empty($checkpoints[$shardId])) {
            return ['ShardIteratorType' => 'TRIM_HORIZON'];
        }

        return ['ShardIteratorType' => 'AFTER_SEQUENCE_NUMBER', 'StartingSequenceNumber' => $checkpoints[$shardId]];
    },
    dataHandler: function ($shardId, $seqNumber, $data) use (&$checkpoints) {
        $checkpoints[$shardId] = $seqNumber;

        print($data . "\n");
    },
    recordsLimit: 1000,
    exceptionHandler: function (Exception $ex) {
        var_dump($ex);
    },
    sleep: 5
);
