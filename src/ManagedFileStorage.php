<?php
/**
 * Nette cache FileStorage with customizable owner and group
 * @author Tomas Polivka <draczris@gmail.com>
 */

namespace ManagedFileStorage;

use Nette,
	Nette\Caching,
	Nette\Caching\Cache,
	Nette\Caching\Storages\FileStorage;

/**
 * Cache file storage with customizable owner and group
 *
 * @author     Tomas Polivka (Heavily inspired by Nette's FileStorage by David Grudl)
 */
class ManagedFileStorage extends Nette\Object implements Nette\Caching\IStorage
{
	/** @var float  probability that the clean() routine is started */
	public static $gcProbability = 0.001;

	/** @var bool */
	public static $useDirectories = TRUE;

	/** @var FilePermissionsManager */
	protected $filePermissionsManager;

	/** @var string */
	protected $dir;

	/** @var bool */
	protected $useDirs;

	/** @var IJournal */
	protected $journal;

	/** @var array */
	protected $locks;


	public function __construct($dir, FilePermissionsManager $filePermissionsManager, IJournal $journal = NULL)
	{
		$this->filePermissionsManager = $filePermissionsManager;
		$this->dir = realpath($dir);
		if ($this->dir === FALSE) {
			throw new Nette\DirectoryNotFoundException("Directory '$dir' not found.");
		}

		$this->useDirs = (bool) self::$useDirectories;
		$this->journal = $journal;

		if (mt_rand() / mt_getrandmax() < self::$gcProbability) {
			$this->clean(array());
		}
	}

	public function setJournal(IJournal $journal)
	{
		$this->journal = $journal;
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
			$this->filePermissionsManager->fixOwnership($dir); // change ownership of cache dir
		}
		$handle = @fopen($cacheFile, 'r+b'); // @ - file may not exist
		if (!$handle) {
			$handle = fopen($cacheFile, 'wb');

			if (!$handle) {
				return;
			}

			fclose($handle);
			$this->filePermissionsManager->fixOwnership($cacheFile); // change ownership of cache file
			$handle = fopen($cacheFile, 'wb');

			if (!$handle) {
				return;
			}
		}

		$this->locks[$key] = $handle;
		flock($handle, LOCK_EX);
	}

	/**
	 * Read from cache.
	 * @param  string key
	 * @return mixed|NULL
	 */
	public function read($key)
	{
		$meta = $this->readMetaAndLock($this->getCacheFile($key), LOCK_SH);
		if ($meta && $this->verify($meta)) {
			return $this->readData($meta); // calls fclose()

		} else {
			return NULL;
		}
	}


	/**
	 * Verifies dependencies.
	 * @param  array
	 * @return bool
	 */
	protected function verify($meta)
	{
		do {
			if (!empty($meta[FileStorage::META_DELTA])) {
				// meta[file] was added by readMetaAndLock()
				if (filemtime($meta[FileStorage::FILE]) + $meta[FileStorage::META_DELTA] < time()) {
					break;
				}
				touch($meta[FileStorage::FILE]);

			} elseif (!empty($meta[FileStorage::META_EXPIRE]) && $meta[FileStorage::META_EXPIRE] < time()) {
				break;
			}

			if (!empty($meta[FileStorage::META_CALLBACKS]) && !Cache::checkCallbacks($meta[FileStorage::META_CALLBACKS])) {
				break;
			}

			if (!empty($meta[FileStorage::META_ITEMS])) {
				foreach ($meta[FileStorage::META_ITEMS] as $depFile => $time) {
					$m = $this->readMetaAndLock($depFile, LOCK_SH);
					if ($m[FileStorage::META_TIME] !== $time || ($m && !$this->verify($m))) {
						break 2;
					}
				}
			}

			return TRUE;
		} while (FALSE);

		$this->delete($meta[FileStorage::FILE], $meta[FileStorage::HANDLE]); // meta[handle] & meta[file] was added by readMetaAndLock()
		return FALSE;
	}

	/**
	 * Writes item into the cache.
	 * @param  string key
	 * @param  mixed  data
	 * @param  array  dependencies
	 * @return void
	 */
	public function write($key, $data, array $dp)
	{
		$meta = array(
			FileStorage::META_TIME => microtime(),
		);

		if (isset($dp[Cache::EXPIRATION])) {
			if (empty($dp[Cache::SLIDING])) {
				$meta[FileStorage::META_EXPIRE] = $dp[Cache::EXPIRATION] + time(); // absolute time
			} else {
				$meta[FileStorage::META_DELTA] = (int) $dp[Cache::EXPIRATION]; // sliding time
			}
		}

		if (isset($dp[Cache::ITEMS])) {
			foreach ((array) $dp[Cache::ITEMS] as $item) {
				$depFile = $this->getCacheFile($item);
				$m = $this->readMetaAndLock($depFile, LOCK_SH);
				$meta[FileStorage::META_ITEMS][$depFile] = $m[FileStorage::META_TIME]; // may be NULL
				unset($m);
			}
		}

		if (isset($dp[Cache::CALLBACKS])) {
			$meta[FileStorage::META_CALLBACKS] = $dp[Cache::CALLBACKS];
		}

		if (!isset($this->locks[$key])) {
			$this->lock($key);
			if (!isset($this->locks[$key])) {
				return;
			}
		}
		$handle = $this->locks[$key];
		unset($this->locks[$key]);

		$cacheFile = $this->getCacheFile($key);

		if (isset($dp[Cache::TAGS]) || isset($dp[Cache::PRIORITY])) {
			if (!$this->journal) {
				throw new Nette\InvalidStateException('CacheJournal has not been provided.');
			}
			$this->journal->write($cacheFile, $dp);
		}

		ftruncate($handle, 0);

		if (!is_string($data)) {
			$data = serialize($data);
			$meta[FileStorage::META_SERIALIZED] = TRUE;
		}

		$head = serialize($meta) . '?>';
		$head = '<?php //netteCache[01]' . str_pad((string) strlen($head), 6, '0', STR_PAD_LEFT) . $head;
		$headLen = strlen($head);
		$dataLen = strlen($data);

		do {
			if (fwrite($handle, str_repeat("\x00", $headLen), $headLen) !== $headLen) {
				break;
			}

			if (fwrite($handle, $data, $dataLen) !== $dataLen) {
				break;
			}

			fseek($handle, 0);
			if (fwrite($handle, $head, $headLen) !== $headLen) {
				break;
			}

			flock($handle, LOCK_UN);
			fclose($handle);
			return;
		} while (FALSE);

		$this->delete($cacheFile, $handle);
	}


	/**
	 * Removes item from the cache.
	 * @param  string key
	 * @return void
	 */
	public function remove($key)
	{
		unset($this->locks[$key]);
		$this->delete($this->getCacheFile($key));
	}


	/**
	 * Removes items from the cache by conditions & garbage collector.
	 * @param  array  conditions
	 * @return void
	 */
	public function clean(array $conditions)
	{
		$all = !empty($conditions[Cache::ALL]);
		$collector = empty($conditions);

		// cleaning using file iterator
		if ($all || $collector) {
			$now = time();
			foreach (Nette\Utils\Finder::find('_*')->from($this->dir)->childFirst() as $entry) {
				$path = (string) $entry;
				if ($entry->isDir()) { // collector: remove empty dirs
					@rmdir($path); // @ - removing dirs is not necessary
					continue;
				}
				if ($all) {
					$this->delete($path);

				} else { // collector
					$meta = $this->readMetaAndLock($path, LOCK_SH);
					if (!$meta) {
						continue;
					}

					if ((!empty($meta[FileStorage::META_DELTA]) && filemtime($meta[FileStorage::FILE]) + $meta[FileStorage::META_DELTA] < $now)
						|| (!empty($meta[FileStorage::META_EXPIRE]) && $meta[FileStorage::META_EXPIRE] < $now)
					) {
						$this->delete($path, $meta[FileStorage::HANDLE]);
						continue;
					}

					flock($meta[FileStorage::HANDLE], LOCK_UN);
					fclose($meta[FileStorage::HANDLE]);
				}
			}

			if ($this->journal) {
				$this->journal->clean($conditions);
			}
			return;
		}

		// cleaning using journal
		if ($this->journal) {
			foreach ($this->journal->clean($conditions) as $file) {
				$this->delete($file);
			}
		}
	}


	/**
	 * Reads cache data from disk.
	 * @param  string  file path
	 * @param  int     lock mode
	 * @return array|NULL
	 */
	protected function readMetaAndLock($file, $lock)
	{
		$handle = @fopen($file, 'r+b'); // @ - file may not exist
		if (!$handle) {
			return NULL;
		}

		flock($handle, $lock);

		$head = stream_get_contents($handle, FileStorage::META_HEADER_LEN);
		if ($head && strlen($head) === FileStorage::META_HEADER_LEN) {
			$size = (int) substr($head, -6);
			$meta = stream_get_contents($handle, $size, FileStorage::META_HEADER_LEN);
			$meta = @unserialize($meta); // intentionally @
			if (is_array($meta)) {
				$meta[FileStorage::FILE] = $file;
				$meta[FileStorage::HANDLE] = $handle;
				return $meta;
			}
		}

		flock($handle, LOCK_UN);
		fclose($handle);
		return NULL;
	}


	/**
	 * Reads cache data from disk and closes cache file handle.
	 * @param  array
	 * @return mixed
	 */
	protected function readData($meta)
	{
		$data = stream_get_contents($meta[FileStorage::HANDLE]);
		flock($meta[FileStorage::HANDLE], LOCK_UN);
		fclose($meta[FileStorage::HANDLE]);

		if (empty($meta[FileStorage::META_SERIALIZED])) {
			return $data;
		} else {
			return @unserialize($data); // intentionally @
		}
	}


	/**
	 * Returns file name.
	 * @param  string
	 * @return string
	 */
	protected function getCacheFile($key)
	{
		$file = urlencode($key);
		if ($this->useDirs && $a = strrpos($file, '%00')) { // %00 = urlencode(Nette\Caching\Cache::NAMESPACE_SEPARATOR)
			$file = substr_replace($file, '/_', $a, 3);
		}
		return $this->dir . '/_' . $file;
	}


	/**
	 * Deletes and closes file.
	 * @param  string
	 * @param  resource
	 * @return void
	 */
	protected static function delete($file, $handle = NULL)
	{
		if (@unlink($file)) { // @ - file may not already exist
			if ($handle) {
				flock($handle, LOCK_UN);
				fclose($handle);
			}
			return;
		}

		if (!$handle) {
			$handle = @fopen($file, 'r+'); // @ - file may not exist
		}
		if ($handle) {
			flock($handle, LOCK_EX);
			ftruncate($handle, 0);
			flock($handle, LOCK_UN);
			fclose($handle);
			@unlink($file); // @ - file may not already exist
		}
	}



	public static function fixOwnership($path, $ownership)
	{
		if (is_null($ownership)) {
			return;
		}

		@chown($path, $ownership);
	}
}
