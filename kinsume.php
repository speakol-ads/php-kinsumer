<?php

use Aws\Kinesis\Exception\KinesisException;
use Aws\Kinesis\KinesisClient;

/**
 * kinsum - a very simple kinesis consumer
 *
 * @param KinesisClient $kinesisClient
 * @param string $streamName
 * @param callable $shardIteratorBuilder(string $shardId)
 * @param callable $dataHandler(string $shardId, string $sequenceNumber, string $data)
 * @param int $recordsLimit getRecords Limit
 * @param callable $exceptionHandler
 * @param int $sleep
 * @return void
 */
function kinsume(
    KinesisClient $kinesisClient,
    string $streamName,
    callable $shardIteratorBuilder,
    callable $dataHandler,
    int $recordsLimit = 10000,
    callable $exceptionHandler = null,
    int $sleep = 1
): void
{
    $res = $kinesisClient->describeStream(['StreamName' => $streamName]);
    $shardsIds = $res->search('StreamDescription.Shards[].ShardId');

    foreach ($shardsIds as $shardId) {
        $seqNumber = "";
        $shardIterator = "";
        $millisBehindLatest = 0;
        do {
            if (!$shardIterator) {
                $iteratorConfigs = array_merge([
                'StreamName' => $streamName,
                'ShardId' => $shardId,
            ], call_user_func_array($shardIteratorBuilder, [$shardId]));
                $res = $kinesisClient->getShardIterator($iteratorConfigs);
                $shardIterator = $res->get('ShardIterator');
            }
            try {
                $res = $kinesisClient->getRecords([
                    'Limit' => $recordsLimit,
                    'ShardIterator' => $shardIterator
                ]);
            } catch (KinesisException $ex) {
                if ($exceptionHandler) {
                    call_user_func_array($exceptionHandler, [$ex]);
                }

                continue;
            }

            $shardIterator = $res->get('NextShardIterator');
            $millisBehindLatest = $res->get('MillisBehindLatest');

            foreach ($res->search('Records[].[SequenceNumber, Data]') as $data) {
                list($sequenceNumber, $item) = $data;
                $seqNumber = $sequenceNumber;
                call_user_func_array($dataHandler, [$shardId, $seqNumber, $item]);
            }

            sleep($sleep);
        } while ($millisBehindLatest > 0);
    }
}
