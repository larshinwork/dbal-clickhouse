<?php

declare(strict_types=1);

/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Exception;
use function strtoupper;
use function substr;
use function trim;

/**
 * ClickHouse Connection
 */
class Connection extends \Doctrine\DBAL\Connection
{
    /**
     * @inheritDoc
     */
    public function executeStatement($sql, array $params = [], array $types = [])
    {
        // ClickHouse has no UPDATE or DELETE statements
        $command = strtoupper(substr(trim($sql), 0, 6));
        if ($command === 'UPDATE' || $command === 'DELETE') {
            throw Exception::notSupported($command);
        }
        return parent::executeStatement($sql, $params, $types);
    }

    /**
     * @throws Exception
     */
    public function delete($table, array $criteria, array $types = []) : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function update($table, array $data, array $criteria, array $types = []) : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

    /**
     * @throws Exception
     */
    public function setTransactionIsolation($level) : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function getTransactionIsolation() : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function getTransactionNestingLevel() : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function transactional(\Closure $func) : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints) : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function getNestTransactionsWithSavepoints() : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function beginTransaction() : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function commit() : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function rollBack() : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function createSavepoint($savepoint) : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function releaseSavepoint($savepoint) : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function rollbackSavepoint($savepoint) : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function setRollbackOnly() : void
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * @throws Exception
     */
    public function isRollbackOnly() : void
    {
        throw Exception::notSupported(__METHOD__);
    }
}
