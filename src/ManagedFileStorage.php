<?php
/**
 * Nette cache FileStorage with customizable owner and group
 * @author Tomas Polivka <draczris@gmail.com>
 */

namespace Draczris;

use Nette,
	Nette\Caching\Cache,
	Nette\Caching\Storages;

/**
 * Cache file storage with customizable owner and group
 *
 * @author     Tomas Polivka
 */
class ManagedFileStorage extends FileStorage implements Nette\Caching\IStorage
{

	protected $ownership;

	public function setCacheOwnership($ownership)
	{
		$this->ownership = $ownership;
	}

	/**
	 * Prevents item reading and writing. Lock is released by write() or remove().
	 * @param  string key
	 * @return void
	 */
	public function lock($key)
	{
		$cacheFile = $this->getCacheFile($key);
		if ($this->useDirs && !is_dir($dir = dirname($cacheFile))) {
			@mkdir($dir); // @ - directory may already exist
			$this->fixOwnership($dir); // change ownership of cache dir
		}
		$handle = @fopen($cacheFile, 'r+b'); // @ - file may not exist
		if (!$handle) {
			$handle = fopen($cacheFile, 'wb');

			if (!$handle) {
				return;
			}

			fclose($handle);
			$this->fixOwnership($cacheFile); // change ownership of cache file
			$handle = fopen($cacheFile, 'wb');

			if (!$handle) {
				return;
			}
		}

		$this->locks[$key] = $handle;
		flock($handle, LOCK_EX);
	}

	protected function fixOwnership($path)
	{
		if (is_null($this->ownership)) {
			return;
		}

		@chown($path, $this->ownership);
	}
}
