<?php

/**
 * Nette cache FileStorage with customizable owner and group
 * @author Tomas Polivka <draczris@gmail.com>
 */

namespace ManagedFileStorage;

use Nette;

class FilePermissionsManager extends Nette\Object
{
	protected $defaultOwnership;

    protected $defaultPermissions;

	protected $useSudo;

	public function __construct($defaultOwnership, $defaultPermissions, $useSudo)
	{
		$this->defaultOwnership = $defaultOwnership;
        $this->defaultPermissions = $defaultPermissions;
		$this->useSudo = $useSudo;
	}

    public function createDirectoryIfNotExists($path, $ownership = NULL, $permissions = NULL)
    {
        if (is_dir($path)) {
            $this->createDirectory($path, $ownership, $permissions);
        }
    }

    public function createFileIfNotExists($path, $ownership = NULL, $permissions = NULL)
    {
        if (!file_exists($path)) {
            $this->createFile($path, $ownership, $permissions);
        }

        return fopen($path, 'r+b');
    }

    public function createDirectory($path, $ownership, $permissions)
    {
        @mkdir($path);

        $this->chmod($path, $permissions, true);
        $this->chown($path, $ownership, true);
    }

    public function createFile($path, $ownership, $permissions)
    {
        $handle = fopen($path, 'wb');
        fclose($path);

        $this->chmod($path, $permissions);
        $this->chown($path, $ownership);
    }

    protected function chmod($path, $permissions, $recursive = false)
    {
        $this->exec(sprintf(
            "chmod %s %s %s",
            $recursive ? "-R" : "",
            $permissions,
            $path
        ));
    }

    protected function chown($path, $ownership, $recursive = false)
    {
        $this->exec(sprintf(
            "chown %s %s %s",
            $recursive ? "-R" : "",
            $ownership,
            $path
        ));
    }

    protected function exec($command, $useSudo = NULL)
    {
        $useSudo = is_null($useSudo) ? $this->useSudo : $useSudo;

        $command = ( $useSudo ? "sudo " : "" ) . $command;

        @shell_exec($command);
    }
}
