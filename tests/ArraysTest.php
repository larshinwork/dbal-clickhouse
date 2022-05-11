<?php
/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse\Tests;

use Doctrine\DBAL\Schema\Table;
use FOD\DBALClickHouse\Connection;
use FOD\DBALClickHouse\Types\ArrayType;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Testing work with array (insert, select)
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class ArraysTest extends TestCase
{
    /** @var  Connection */
    protected $connection;

    public function setUp(): void
    {
        $this->connection = CreateConnectionTest::createConnection();
        ArrayType::registerArrayTypes($this->connection->getDatabasePlatform());
    }

    public function tearDown(): void
    {
        $this->connection->executeStatement('DROP TABLE test_array_table');
    }

    public function testArrayInt8()
    {
        $this->createTempTable('array(int8)');
        $this->connection->insert('test_array_table', ['arr' => [1, 2, 3, 4, 5, 6, 7, 8]]);
        $this->assertEquals(['arr' => [1, 2, 3, 4, 5, 6, 7, 8]], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayInt16()
    {
        $this->createTempTable('array(int16)');
        $this->connection->insert('test_array_table', ['arr' => [100, 2000, 30000]]);
        $this->assertEquals(['arr' => [100, 2000, 30000]], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayInt32()
    {
        $this->createTempTable('array(int32)');
        $this->connection->insert('test_array_table', ['arr' => [1000000, 2000000000]]);
        $this->assertEquals(['arr' => [1000000, 2000000000]], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayInt64()
    {
        $this->createTempTable('array(int64)');
        $this->connection->insert('test_array_table', ['arr' => [200000000000, 3000000000000000000]]);
        $this->assertEquals(['arr' => [200000000000, 3000000000000000000]], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayUInt8()
    {
        $this->createTempTable('array(uint8)');
        $this->connection->insert('test_array_table', ['arr' => [1, 2, 3, 4, 5, 6, 7, 8]]);
        $this->assertEquals(['arr' => [1, 2, 3, 4, 5, 6, 7, 8]], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayUInt16()
    {
        $this->createTempTable('array(uint16)');
        $this->connection->insert('test_array_table', ['arr' => [100, 2000, 30000]]);
        $this->assertEquals(['arr' => [100, 2000, 30000]], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayUInt32()
    {
        $this->createTempTable('array(uint32)');
        $this->connection->insert('test_array_table', ['arr' => [1000000, 2000000000]]);
        $this->assertEquals(['arr' => [1000000, 2000000000]], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayUInt64()
    {
        $this->createTempTable('array(uint64)');
        $this->connection->insert('test_array_table', ['arr' => [200000000000, 3000000000000000000]]);
        $this->assertEquals(['arr' => [200000000000, 3000000000000000000]], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayFloat32()
    {
        $this->createTempTable('array(float32)');
        $this->connection->insert('test_array_table', ['arr' => [1.5, 10.5]]);
        $this->assertEquals(['arr' => [1.5, 10.5]], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayFloat64()
    {
        $this->createTempTable('array(float64)');
        $this->connection->insert('test_array_table', ['arr' => [100.512, 10000.5814]]);
        $this->assertEquals(['arr' => [100.512, 10000.5814]], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayString()
    {
        $this->createTempTable('array(string)');
        $this->connection->insert('test_array_table', ['arr' => ['foo', 'bar']]);
        $this->assertEquals(['arr' => ['foo', 'bar']], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayDatetime()
    {
        $dateTimeArray = [(new \DateTime('2000-01-01'))->format('Y-m-d H:i:s'), (new \DateTime('2017-05-05'))->format('Y-m-d H:i:s')];
        $this->createTempTable('array(datetime)');
        $this->connection->insert('test_array_table', ['arr' => $dateTimeArray]);
        $this->assertEquals(['arr' => $dateTimeArray], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    public function testArrayDate()
    {
        $datesArray = [(new \DateTime('2000-01-01'))->format('Y-m-d'), (new \DateTime('2017-05-05'))->format('Y-m-d')];
        $this->createTempTable('array(date)');
        $this->connection->insert('test_array_table', ['arr' => $datesArray]);
        $this->assertEquals(['arr' => $datesArray], current($this->connection->executeQuery('SELECT arr FROM test_array_table')->fetchAllAssociative()));
    }

    protected function createTempTable($arrayType)
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;
        if ($toSchema->hasTable('test_array_table') || $fromSchema->hasTable('test_array_table')) {
            $this->connection->executeStatement('DROP TABLE test_array_table');
        }
        $newTable = $toSchema->createTable('test_array_table');

        $newTable->addColumn('arr', $arrayType);
        $newTable->addOption('engine', 'Memory');

        foreach ($fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform()) as $sql) {
            $this->connection->executeStatement($sql);
        }
    }
}

