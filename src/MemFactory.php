<?php declare(strict_types=1);

namespace Swoft\Swlib;

use InvalidArgumentException;

/**
 * Class MemFactory
 */
final class MemFactory
{
    /**
     * @var MemTable[]
     */
    private static $tables = [];

    /**
     * @param string $name
     * @param int    $size
     * @param array  $columns ['field' => ['type', 'size']]
     *
     * @return MemTable
     */
    public static function create(string $name, int $size = 0, array $columns = []): MemTable
    {
        $table = new MemTable($name, $size, $columns);

        // Save instance
        self::$tables[$name] = $table;
        return $table;
    }

    /**
     * @param string $name
     *
     * @return MemTable
     */
    public static function get(string $name): MemTable
    {
        if (isset(self::$tables[$name])) {
            return self::$tables[$name];
        }

        throw new InvalidArgumentException('The memory table instance is not exists');
    }

    /**
     * @param string   $name
     * @param MemTable $table
     */
    public static function set(string $name, MemTable $table): void
    {
        self::$tables[$name] = $table;
    }

    /**
     * @param string $name
     *
     * @return bool
     */
    public static function del(string $name): bool
    {
        if (isset(self::$tables[$name])) {
            unset(self::$tables[$name]);
            return true;
        }

        return false;
    }

    /**
     * Clear all tables
     *
     * @param bool $clearData
     */
    public static function clear(bool $clearData = true): void
    {
        foreach (self::$tables as $name => $table) {
            if ($clearData) {
                $table->clear();
            }

            unset(self::$tables[$name]);
        }
    }
}
