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

use ClickHouseDB\Exception\DatabaseException;
use ClickHouseDB\Exception\QueryException;
use Doctrine\DBAL\Exception;
use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\Table;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\Types;
use FOD\DBALClickHouse\ClickHouseSchemaManager;
use FOD\DBALClickHouse\Connection;
use FOD\DBALClickHouse\Types\ArrayType;
use PHPUnit\Framework\TestCase;

/**
 * ClickHouse DBAL test class. Tests of Schema manager for create table in ClickHouse
 *
 * @author Nikolay Mitrofanov <mitrofanovnk@gmail.com>
 */
class CreateSchemaTest extends TestCase
{
    /** @var  Connection */
    protected $connection;

    public function setUp(): void
    {
        $this->connection = CreateConnectionTest::createConnection();
    }

    public function testGetSchemaManager()
    {
        $this->assertInstanceOf(ClickHouseSchemaManager::class, $this->connection->createSchemaManager());
    }

    public function testCreateNewTableSQL()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('oneVal', Types::FLOAT);
        $newTable->addColumn('twoVal', Types::DECIMAL);
        $newTable->addColumn('flag', Types::BOOLEAN);
        $newTable->addColumn('mask', Types::SMALLINT);
        $newTable->addColumn('hash', 'string', ['length' => 32, 'fixed' => true]);
        $newTable->setPrimaryKey(['id']);

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $this->assertEquals("CREATE TABLE test_table (EventDate Date DEFAULT today(), id UInt32, payload String, oneVal Float64, twoVal String, flag UInt8, mask Int16, hash FixedString(32)) ENGINE = ReplacingMergeTree(EventDate, (id), 8192)",
            implode(';', $migrationSQLs));
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }
        $this->connection->executeStatement('DROP TABLE test_table');
    }

    public function testCreateDropTable()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('oneVal', Types::FLOAT);
        $newTable->addColumn('twoVal', Types::DECIMAL);
        $newTable->addColumn('flag', Types::BOOLEAN);
        $newTable->addColumn('mask', Types::SMALLINT);
        $newTable->addColumn('hash', 'string', ['length' => 32, 'fixed' => true]);
        $newTable->setPrimaryKey(['id']);

        foreach ($fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform()) as $sql) {
            $this->connection->executeStatement($sql);
        }

        $this->connection->executeStatement('DROP TABLE test_table');
        $this->expectException(QueryException::class);
        $this->connection->executeStatement('DROP TABLE test_table');
    }

    public function testIndexGranularityOption()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->setPrimaryKey(['id']);
        $newTable->addOption('indexGranularity', 4096);

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (EventDate Date DEFAULT today(), id UInt32, payload String) ENGINE = ReplacingMergeTree(EventDate, (id), 4096)",
            $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }
        $this->connection->executeStatement('DROP TABLE test_table');
    }

    public function testEngineMergeOption()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->setPrimaryKey(['id']);
        $newTable->addOption('engine', 'MergeTree');

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (EventDate Date DEFAULT today(), id UInt32, payload String) ENGINE = MergeTree(EventDate, (id), 8192)",
            $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }
        $this->connection->executeStatement('DROP TABLE test_table');
    }

    public function testEngineMemoryOption()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->setPrimaryKey(['id']);
        $newTable->addOption('engine', 'Memory');

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (id UInt32, payload String) ENGINE = Memory", $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }
        $this->connection->executeStatement('DROP TABLE test_table');
    }

    public function testEventDateColumnOption()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('event_date', 'date', ['default' => 'toDate(now())']);
        $newTable->addOption('eventDateColumn', 'event_date');
        $newTable->setPrimaryKey(['id']);

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (event_date Date DEFAULT today(), id UInt32, payload String) ENGINE = ReplacingMergeTree(event_date, (id), 8192)",
            $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }
        $this->connection->executeStatement('DROP TABLE test_table');
    }

    public function testEventDateColumnBadOption()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('event_date', Types::DATETIME_MUTABLE, ['default' => 'toDate(now())']);
        $newTable->addOption('eventDateColumn', 'event_date');
        $newTable->setPrimaryKey(['id']);

        $this->expectException(\Exception::class);
        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (event_date Date DEFAULT today(), id UInt32, payload String) ENGINE = ReplacingMergeTree(event_date, (id), 8192)",
            $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }
        $this->connection->executeStatement('DROP TABLE test_table');
    }

    public function testEventDateProviderColumnOption()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('updated_at', Types::DATETIME_MUTABLE);
        $newTable->addOption('eventDateProviderColumn', 'updated_at');
        $newTable->setPrimaryKey(['id']);

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (EventDate Date DEFAULT toDate(updated_at), id UInt32, payload String, updated_at DateTime) ENGINE = ReplacingMergeTree(EventDate, (id), 8192)",
            $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }
        $this->connection->executeStatement('DROP TABLE test_table');
    }

    public function testEventDateProviderColumnBadOption()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('flag', Types::BOOLEAN);
        $newTable->addOption('eventDateProviderColumn', 'flag');
        $newTable->setPrimaryKey(['id']);

        $this->expectException(\Exception::class);
        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table (EventDate Date DEFAULT toDate(updated_at), id UInt32, payload String, updated_at DateTime) ENGINE = ReplacingMergeTree(EventDate, (id), 8192)",
            $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }
        $this->connection->executeStatement('DROP TABLE test_table');
    }

    public function testListTableIndexes()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_indexes_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('event_date', Types::DATE_MUTABLE);
        $newTable->addOption('eventDateColumn', 'event_date');
        $newTable->setPrimaryKey(['id', 'event_date']);
        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }

        $indexes = $this->connection->createSchemaManager()->listTableIndexes('test_indexes_table');

        $this->assertEquals(1, \count($indexes));

        if ($index = current($indexes)) {
            $this->assertInstanceOf(Index::class, $index);

            $this->assertEquals(['id', 'event_date'], $index->getColumns());
            $this->assertTrue($index->isPrimary());
        }

        $this->connection->executeStatement('DROP TABLE test_indexes_table');
    }

    public function testTableWithSamplingExpression()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_sampling_table');

        $newTable->addColumn('id', 'integer', ['unsigned' => true]);
        $newTable->addColumn('payload', 'string');
        $newTable->addColumn('event_date', Types::DATE_MUTABLE);
        $newTable->addOption('eventDateColumn', 'event_date');
        $newTable->addOption('samplingExpression', 'intHash32(id)');
        $newTable->setPrimaryKey(['id', 'event_date']);
        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_sampling_table (event_date Date DEFAULT today(), id UInt32, payload String) ENGINE = ReplacingMergeTree(event_date, intHash32(id), (id, event_date, intHash32(id)), 8192)",
            $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }

        $indexes = $this->connection->createSchemaManager()->listTableIndexes('test_sampling_table');

        $this->assertEquals(1, \count($indexes));

        if ($index = current($indexes)) {
            $this->assertInstanceOf(Index::class, $index);

            $this->assertEquals(['id', 'event_date'], $index->getColumns());
            $this->assertTrue($index->isPrimary());
        }

        $this->connection->executeStatement('DROP TABLE test_sampling_table');
    }

    public function testNullableColumns()
    {
        $fromSchema = $this->connection->createSchemaManager()->createSchema();
        ArrayType::registerArrayTypes($this->connection->getDatabasePlatform());

        $toSchema = clone $fromSchema;

        $newTable = $toSchema->createTable('test_table_nullable');

        $newTable->addColumn('id', 'integer', ['unsigned' => true, 'notnull' => false]);
        $newTable->addColumn('payload', 'string', ['notnull' => false]);
        $newTable->addColumn('price', 'float', ['notnull' => false]);
        $newTable->addColumn('transactions', 'array(datetime)', ['notnull' => false]);
        $newTable->addColumn('status', 'boolean', ['notnull' => false]);
        $newTable->setPrimaryKey(['id']);
        $newTable->addOption('engine', 'Memory');

        $migrationSQLs = $fromSchema->getMigrateToSql($toSchema, $this->connection->getDatabasePlatform());
        $generatedSQL = implode(';', $migrationSQLs);
        $this->assertEquals("CREATE TABLE test_table_nullable (id UInt32, payload Nullable(String), price Nullable(Float64), transactions Array(Nullable(DateTime)), status Nullable(UInt8)) ENGINE = Memory",
            $generatedSQL);
        foreach ($migrationSQLs as $sql) {
            $this->connection->executeStatement($sql);
        }
        $this->connection->insert('test_table_nullable',
            [
                'id' => 1,
                'payload' => 's1',
                'price' => 1.5,
                'transactions' => [date('Y-m-d H:i:s'), null],
                'status' => null
            ]);
        $this->connection->insert('test_table_nullable',
            [
                'id' => 2,
                'payload' => 's2',
                'price' => 120,
                'transactions' => [null, null],
                'status' => false
            ]);
        $this->connection->insert('test_table_nullable',
            [
                'id' => 3,
                'payload' => null,
                'price' => 1000,
                'transactions' => [date('Y-m-d H:i:s')],
                'status' => true
            ]);
        $this->connection->insert('test_table_nullable',
            [
                'id' => 4,
                'payload' => 's4',
                'price' => null,
                'transactions' => [date('Y-m-d H:i:s'), date('Y-m-d H:i:s')],
                'status' => null
            ]);
        $this->connection->insert('test_table_nullable',
            [
                'id' => 5,
                'payload' => 's5',
                'price' => 100,
                'transactions' => [date('Y-m-d H:i:s')],
                'status' => true
            ]);

        $this->assertEquals(2,
            (int)$this->connection->fetchOne("SELECT count() from test_table_nullable WHERE {$this->connection->getDatabasePlatform()->getIsNullExpression('status')}"));

        $this->connection->executeStatement('DROP TABLE test_table_nullable');
    }
}
