<?php

declare(strict_types=1);

/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Exception;

use function mb_strtoupper;
use function mb_substr;
use function trim;

class Connection extends \Doctrine\DBAL\Connection
{
    private int $transactionIsolationLevel = 0;
    /**
     * {@inheritDoc}
     */
    public function delete($table, array $criteria, array $types = []): int
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function update($table, array $data, array $criteria, array $types = []): int
    {
        throw Exception::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function executeStatement($sql, array $params = [], array $types = []): int|string
    {
        $command = mb_strtoupper(mb_substr(trim($sql), 0, 6));

        if (in_array($command, ['DELETE', 'UPDATE'], true)) {
            throw Exception::notSupported($command);
        }

        return parent::executeStatement($sql, $params, $types);
    }

    /**
     * all methods below throw exceptions, because ClickHouse has not transactions
     */

    /**
     * {@inheritDoc}
     */
    public function setTransactionIsolation($level): int
    {
        return $this->transactionIsolationLevel = (int) $level;
    }

    /**
     * {@inheritDoc}
     */
    public function getTransactionIsolation(): int
    {
        return $this->transactionIsolationLevel;
    }

    /**
     * {@inheritDoc}
     */
    public function getTransactionNestingLevel(): int
    {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public function transactional(\Closure $func): void
    {
        $func();
    }

    /**
     * {@inheritDoc}
     */
    public function setNestTransactionsWithSavepoints($nestTransactionsWithSavepoints): void
    {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    public function getNestTransactionsWithSavepoints(): bool
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function beginTransaction(): bool
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function commit(): bool
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function rollBack(): bool
    {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function createSavepoint($savepoint): void
    {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    public function releaseSavepoint($savepoint): void
    {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    public function rollbackSavepoint($savepoint): void
    {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    public function setRollbackOnly(): void
    {
        // no-op
    }

    /**
     * {@inheritDoc}
     */
    public function isRollbackOnly(): bool
    {
        return true;
    }
}
