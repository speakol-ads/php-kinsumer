<?php

use Aws\Kinesis\KinesisClient;

/**
 * kinsum - a very simple kinesis consumer
 * 
 * @param KinesisClient $kinesisClient
 * @param string $streamName
 * @param callable $shardIteratorBuilder(string $shardId)
 * @param callable $dataHandler(string $shardId, string $sequenceNumber, string $data)
 * @param int $recordsLimit getRecords Limit
 * 
 * @return void
 */
function kinsume(KinesisClient $kinesisClient, string $streamName, callable $shardIteratorBuilder, callable $dataHandler, int $recordsLimit = 10000): void
{
    while (true) {
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

                $res = $kinesisClient->getRecords([
                    'Limit' => $recordsLimit,
                    'ShardIterator' => $shardIterator
                ]);

                $shardIterator = $res->get('NextShardIterator');
                $millisBehindLatest = $res->get('MillisBehindLatest');

                foreach ($res->search('Records[].[SequenceNumber, Data]') as $data) {
                    list($sequenceNumber, $item) = $data;
                    $seqNumber = $sequenceNumber;
                    call_user_func_array($dataHandler, [$shardId, $seqNumber, $item]);
                }
            } while ($millisBehindLatest > 0);
        }
    }
}
