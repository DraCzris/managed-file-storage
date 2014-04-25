<?php

/**
 * Nette cache FileStorage with customizable owner and group
 * @author Tomas Polivka <draczris@gmail.com>
 */

namespace ManagedFileStorage;

use Nette;

class FilePermissionsManager extends Nette\Object
{
	protected $ownership;

	public __construct($ownership)
	{
		$this->$ownership = $ownership;
	}

	public function fixOwnership($path)
	{
		if (is_null($this->ownership)) {
			return;
		}

		@chown($path, $this->ownership);
	}
}
