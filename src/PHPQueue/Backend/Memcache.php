<?php
namespace PHPQueue\Backend;

use PHPQueue\Exception\BackendException;
use PHPQueue\Interfaces\KeyValueStore;

class Memcache
    extends Base
    implements KeyValueStore
{
    public $servers;
    public $is_persistent = false;
    public $use_compression = false;
    public $expiry = 0;
    public $queue_name;

    public function __construct($options=array())
    {
        parent::__construct();
        if (!empty($options['servers']) && is_array($options['servers'])) {
            $this->servers = $options['servers'];
        }
        if (!empty($options['persistent'])) {
            $this->is_persistent = $options['persistent'];
        }
        if (!empty($options['compress']) && is_bool($options['compress'])) {
            $this->use_compression = $options['compress'];
        }
        if (!empty($options['expiry']) && is_numeric($options['expiry'])) {
            $this->expiry = $options['expiry'];
        }
        // Note: queue_name must be set in order to get FifoQueue behavior.
        if (!empty($options['queue']) && is_string($options['queue'])) {
            $this->queue_name = $options['queue'];
        }
    }

    public function connect()
    {
        if (empty($this->servers)) {
            throw new BackendException("No servers specified");
        }
        $this->connection = new \Memcache;
        foreach ($this->servers as $server) {
            if (is_string($server)) {
                // TODO: configure port
                $this->connection->addserver($server, 11211, $this->is_persistent);
            } elseif (is_array($server)) {
                call_user_func_array(array($this->connection, 'addserver'), $server);
            } else {
                throw new BackendException("Unknown Memcache server arguments.");
            }
        }
    }

    /**
     * @deprecated Use set($k, $v) and $this->expiry instead.
     * @param  string              $key
     * @param  mixed               $data
     * @param  int                 $expiry
     * @return boolean
     * @throws \PHPQueue\Exception
     */
    public function add($key, $data, $expiry=null)
    {
        $this->set($key, json_encode($data), $expiry);
        return true;
    }

    /**
     * @param  string              $key
     * @param  mixed               $data
     * @param  int                 $expiry Deprecated param
     * @throws \PHPQueue\Exception
     */
    public function set($key, $data, $expiry=null)
    {
        if (empty($key) && !is_string($key)) {
            throw new BackendException("Key is invalid.");
        }
        if (empty($data)) {
            throw new BackendException("No data.");
        }
        $this->beforeAdd();
        if (is_array($expiry)) {
            // FIXME: Silently swallows incompatible $properties argument.
            $expiry = null;
        }
        if (empty($expiry)) {
            $expiry = $this->expiry;
        }
        if ($this->queue_name) {
            $contents = $this->get_bucket_contents();
            $contents[$key] = true;
            $this->set_bucket_contents($contents);
        }
        $this->replace_or_set($key, $data, $expiry);
    }

    /**
     * @param  string $key
     * @return mixed
     */
    public function get($key)
    {
        $this->beforeGet($key);

        return json_decode($this->getConnection()->get($key), true);
    }

    public function clear($key)
    {
        $this->beforeClear($key);
        $this->getConnection()->delete($key);
        $this->last_job_id = $key;

        if ($this->queue_name) {
            $contents = $this->get_bucket_contents();
            unset($contents[$key]);
            $this->set_bucket_contents($contents);
        }

        return true;
    }

    protected function bucket_key() {
        return $this->queue_name . ':contents';
    }

    // FIXME: Do we want to make this part of the API?
    public function get_bucket_contents() {
        $contents = $this->getConnection()->get($this->bucket_key());
        if ($contents) {
            $contents = json_decode($contents, true);
        } else {
            $contents = array();
        }
        return $contents;
    }

    public function set_bucket_contents($contents) {
        $this->replace_or_set($this->bucket_key(), $contents, 60 * 60 * 24 * 30);
    }

    protected function replace_or_set($key, $data, $expiry) {
        $encoded = json_encode($data);

        $status = $this->getConnection()->replace($key, $encoded, $this->use_compression, $expiry);
        if ($status !== true) {
            $status = $this->getConnection()->set($key, $encoded, $this->use_compression, $expiry);
        }
        if ($status !== true) {
            throw new BackendException("Unable to save data.");
        }
    }
}
