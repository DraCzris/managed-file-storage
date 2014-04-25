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

	protected $useSudo;

	public function __construct($defaultOwnership, $useSudo)
	{
		$this->defaultOwnership = $defaultOwnership;
		$this->useSudo = $useSudo;
	}

	public function fixOwnership($path)
	{
		$this->changeOwnership($path);
	}

	protected function changeOwnership($path, $ownership = NULL)
	{
		if (is_null($ownership)) {
			$ownership = $this->defaultOwnership;
		}

		if (is_null($ownership)) {
			return;
		}

		if ($this->userSudo) {
			@system(sprintf('sudo chown %s %s', $ownership, $path));
		} else {
			@chown($path, $ownership);
		}
	}
}
