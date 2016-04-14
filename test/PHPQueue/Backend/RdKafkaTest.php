<?php
namespace PHPQueue\Backend;

use PHPUnit_Framework_TestCase;

class RdKafkaTest extends PHPUnit_Framework_TestCase
{
    private $object;

    public function setUp()
    {
        parent::setUp();
        if (!class_exists('\RdKafka\Producer')) {
            $this->markTestSkipped('RdKafka PHP extension not installed');
        } else {
            $options = array(
                'servers' => array('127.0.0.1:9092')
                , 'queue' => 'testqueue'
            );
            $this->object = new RdKafka($options);
        }
    }

    public function testPushPop()
    {
        $data = 'Weezle-' . mt_rand();
        $this->object->push($data);

        $this->assertEquals($data, $this->object->pop());

        // Check that we did remove the object.
        $this->assertNull($this->object->pop());
    }

    public function testPopEmpty()
    {
        $this->assertNull($this->object->pop());
    }
}
