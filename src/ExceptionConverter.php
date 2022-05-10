<?php

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Driver\Exception;
use Doctrine\DBAL\Exception\DriverException;
use Doctrine\DBAL\Query;

/**
 * @internal
 */
final class ExceptionConverter implements \Doctrine\DBAL\Driver\API\ExceptionConverter
{

    /**
     * @inheritDoc
     */
    public function convert(Exception $exception, ?Query $query): DriverException
    {
        return new ClickHouseDriverException($exception, $query);
    }
}