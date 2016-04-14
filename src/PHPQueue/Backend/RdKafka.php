<?php
namespace PHPQueue\Backend;

use RdKafka\Conf;
use RdKafka\Consumer;
use RdKafka\Producer;
use RdKafka\TopicConf;

use PHPQueue\Exception\BackendException;
use PHPQueue\Interfaces\FifoQueueStore;

/**
 * TODO:
 *   - Use partitions.
 */
class RdKafka
    extends Base
    implements FifoQueueStore
{
    public $servers;
    public $queue_name;
    public $consumer;
    public $consumer_topic;
    public $producer;
    public $producer_topic;

    public function __construct($options=array())
    {
        parent::__construct();
        if (!empty($options['servers'])) {
            $this->servers = $options['servers'];
        }
        if (!empty($options['queue'])) {
            $this->queue_name = $options['queue'];
        }
    }

    public function connect()
    {
        if (!$this->servers) {
            throw new BackendException("No servers specified");
        }
        // Connections are opened on-demand, once we know if this will be a
        // producer or consumer.
    }

    public function getProducer() {
        if (!$this->producer) {
            $this->producer = new Producer();
            $this->producer->setLogLevel(LOG_DEBUG);
            $this->producer->addBrokers(implode(",", $this->servers));
        }

        return $this->producer;
    }

    public function getProducerTopic() {
        if (!$this->producer_topic) {
            $this->producer_topic = $this->getProducer()->newTopic($this->queue_name);
        }

        return $this->producer_topic;
    }

    public function getConsumer() {
        if (!$this->consumer) {
            $conf = new Conf();
            // TODO: group id

            $this->consumer = new Consumer($conf);
            $this->consumer->addBrokers(implode(",", $this->servers));
        }

        return $this->consumer;
    }

    public function getConsumerTopic() {
        if (!$this->consumer_topic) {
            $topicConf = new TopicConf();
            // TODO: set topic conf

            $this->consumer_topic = $this->getConsumer()->newTopic($this->queue_name, $topicConf);
            $this->consumer_topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
        }

        return $this->consumer_topic;
    }

    /** @deprecated */
    public function add($data=array())
    {
        if (!$data) {
            throw new BackendException("No data.");
        }
        $this->push($data);
        return true;
    }

    public function push($data)
    {
        $this->beforeAdd();
        if (!$this->hasQueue()) {
            throw new BackendException("No queue specified.");
        }
        $encoded_data = json_encode($data);

        $this->getProducerTopic()->produce(RD_KAFKA_PARTITION_UA, 0, $encoded_data);
    }

    /**
     * @return array|null
     */
    public function pop()
    {
        $this->beforeGet();
        if (!$this->hasQueue()) {
            throw new BackendException("No queue specified.");
        }

        // TODO: configurable timeout
        $message = $this->getConsumerTopic()->consume(0, 120*10000);
        switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            $data = $message->payload;
            break;
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            $data = null;
            break;
        default:
            throw new BackendException("[{$message->err}]: {$message->errstr()}");
        }

        if (!$data) {
            return null;
        }
        $this->last_job = $data;
        $this->last_job_id = time();
        $this->afterGet();

        return json_decode($data, true);
    }

    /**
     * Return the top element in the queue.
     *
     * @return array|null
     */
    public function peek()
    {
        $data = null;
        $this->beforeGet();
        if (!$this->hasQueue()) {
            throw new BackendException("No queue specified.");
        }
        // TODO
        if (!$data) {
            return null;
        }
        $this->last_job = $data;
        $this->last_job_id = time();
        $this->afterGet();

        return json_decode($data, true);
    }

    public function hasQueue()
    {
        return !empty($this->queue_name);
    }
}
