<?php

/**
 * Nette cache FileStorage with customizable owner and group
 * @author Tomas Polivka <draczris@gmail.com>
 */

namespace ManagedFileStorage;

use Nette,
	Nette\Caching\Cache,
	Nette\Caching\Storages\FileJournal,
	Nette\Caching\Storages\IJournal;


/**
 * Btree+ based file journal.
 *
 * @author     Tomas Polivka (Heavily inspired by Nette's FileJournal by Jakub Onderka)
 */
class ManagedFileJournal extends Nette\Object implements IJournal
{

	/** Debug mode, only for testing purposes */
	public static $debug = FALSE;

	/** @var string */
	protected $file;

	/** @var resource */
	protected $handle;

	/** @var int Last complete free node */
	protected $lastNode = 2;

	/** @var string */
	protected $processIdentifier;

	/** @var array Cache and uncommitted but changed nodes */
	protected $nodeCache = array();

	/** @var array */
	protected $nodeChanged = array();

	/** @var array */
	protected $toCommit = array();

	/** @var array */
	protected $deletedLinks = array();

	/** @var array Free space in data nodes */
	protected $dataNodeFreeSpace = array();

	/** @var FilePermissionsManager */
	protected $filePermissionsManager;


	/** @var array */
	protected static $startNode = array(
		FileJournal::TAGS     => 0,
		FileJournal::PRIORITY => 1,
		FileJournal::ENTRIES  => 2,
		FileJournal::DATA     => 3,
	);


	/**
	 * @param  string  Directory containing journal file
	 */
	public function __construct($dir, FilePermissionsManager $filePermissionsManager)
	{
		$this->filePermissionsManager = $filePermissionsManager;
		$this->file = $dir . '/' . FileJournal::FILE;
	}


	/**
	 * @return void
	 */
	public function __destruct()
	{
		if ($this->handle) {
			$this->headerCommit();
			flock($this->handle, LOCK_UN); // Since PHP 5.3.3 is manual unlock necessary
			fclose($this->handle);
			$this->handle = FALSE;
		}
	}


	/**
	 * Writes entry information into the journal.
	 * @param  string
	 * @param  array
	 * @return void
	 */
	public function write($key, array $dependencies)
	{
		$this->lock();

		$priority = !isset($dependencies[Cache::PRIORITY]) ? FALSE : (int) $dependencies[Cache::PRIORITY];
		$tags = empty($dependencies[Cache::TAGS]) ? FALSE : (array) $dependencies[Cache::TAGS];

		$exists = FALSE;
		$keyHash = crc32($key);
		list($entriesNodeId, $entriesNode) = $this->findIndexNode(FileJournal::ENTRIES, $keyHash);

		if (isset($entriesNode[$keyHash])) {
			$entries = $this->mergeIndexData($entriesNode[$keyHash]);
			foreach ($entries as $link => $foo) {
				$dataNode = $this->getNode($link >> FileJournal::BITROT);
				if ($dataNode[$link][FileJournal::KEY] === $key) {
					if ($dataNode[$link][FileJournal::TAGS] == $tags && $dataNode[$link][FileJournal::PRIORITY] === $priority) { // intentionally ==, the order of tags does not matter
						if ($dataNode[$link][FileJournal::DELETED]) {
							$dataNode[$link][FileJournal::DELETED] = FALSE;
							$this->saveNode($link >> FileJournal::BITROT, $dataNode);
						}
						$exists = TRUE;
					} else { // Already exists, but with other tags or priority
						$toDelete = array();
						foreach ($dataNode[$link][FileJournal::TAGS] as $tag) {
							$toDelete[FileJournal::TAGS][$tag][$link] = TRUE;
						}
						if ($dataNode[$link][FileJournal::PRIORITY] !== FALSE) {
							$toDelete[FileJournal::PRIORITY][$dataNode[$link][FileJournal::PRIORITY]][$link] = TRUE;
						}
						$toDelete[FileJournal::ENTRIES][$keyHash][$link] = TRUE;
						$this->cleanFromIndex($toDelete);

						unset($dataNode[$link]);
						$this->saveNode($link >> FileJournal::BITROT, $dataNode);

						// Node was changed but may be empty, find it again
						list($entriesNodeId, $entriesNode) = $this->findIndexNode(FileJournal::ENTRIES, $keyHash);
					}
					break;
				}
			}
		}

		if ($exists === FALSE) {
			// Magical constants
			$requiredSize = strlen($key) + 75;
			if ($tags) {
				foreach ($tags as $tag) {
					$requiredSize += strlen($tag) + 13;
				}
			}
			$requiredSize += $priority ? 10 : 1;

			$freeDataNode = $this->findFreeDataNode($requiredSize);
			$data = $this->getNode($freeDataNode);

			if ($data === FALSE) {
				$data = array(
					FileJournal::INFO => array(
						FileJournal::LAST_INDEX => ($freeDataNode << FileJournal::BITROT),
						FileJournal::TYPE => FileJournal::DATA,
					)
				);
			}

			$dataNodeKey = $this->findNextFreeKey($freeDataNode, $data);
			$data[$dataNodeKey] = array(
				FileJournal::KEY => $key,
				FileJournal::TAGS => $tags ? $tags : array(),
				FileJournal::PRIORITY => $priority,
				FileJournal::DELETED => FALSE,
			);

			$this->saveNode($freeDataNode, $data);

			// Save to entries tree, ...
			$entriesNode[$keyHash][$dataNodeKey] = 1;
			$this->saveNode($entriesNodeId, $entriesNode);

			// ...tags tree...
			if ($tags) {
				foreach ($tags as $tag) {
					list($nodeId, $node) = $this->findIndexNode(FileJournal::TAGS, $tag);
					$node[$tag][$dataNodeKey] = 1;
					$this->saveNode($nodeId, $node);
				}
			}

			// ...and priority tree.
			if ($priority !== FALSE) {
				list($nodeId, $node) = $this->findIndexNode(FileJournal::PRIORITY, $priority);
				$node[$priority][$dataNodeKey] = 1;
				$this->saveNode($nodeId, $node);
			}
		}

		$this->commit();
		$this->unlock();
	}


	/**
	 * Cleans entries from journal.
	 * @param  array
	 * @return array of removed items or NULL when performing a full cleanup
	 */
	public function clean(array $conditions)
	{
		$this->lock();

		if (!empty($conditions[Cache::ALL])) {
			$this->nodeCache = $this->nodeChanged = $this->dataNodeFreeSpace = array();
			$this->deleteAll();
			$this->unlock();
			return NULL;
		}

		$toDelete = array(
			FileJournal::TAGS => array(),
			FileJournal::PRIORITY => array(),
			FileJournal::ENTRIES => array()
		);

		$entries = array();

		if (!empty($conditions[Cache::TAGS])) {
			$entries = $this->cleanTags((array) $conditions[Cache::TAGS], $toDelete);
		}

		if (isset($conditions[Cache::PRIORITY])) {
			$this->arrayAppend($entries, $this->cleanPriority((int) $conditions[Cache::PRIORITY], $toDelete));
		}

		$this->deletedLinks = array();
		$this->cleanFromIndex($toDelete);

		$this->commit();
		$this->unlock();

		return $entries;
	}


	/**
	 * Cleans entries from journal by tags.
	 * @param  array
	 * @param  array
	 * @return array of removed items
	 */
	protected function cleanTags(array $tags, array & $toDelete)
	{
		$entries = array();

		foreach ($tags as $tag) {
			list(, $node) = $this->findIndexNode(FileJournal::TAGS, $tag);

			if (isset($node[$tag])) {
				$ent = $this->cleanLinks($this->mergeIndexData($node[$tag]), $toDelete);
				$this->arrayAppend($entries, $ent);
			}
		}

		return $entries;
	}


	/**
	 * Cleans entries from journal by priority.
	 * @param  integer
	 * @param  array
	 * @return array of removed items
	 */
	protected function cleanPriority($priority, array & $toDelete)
	{
		list(, $node) = $this->findIndexNode(FileJournal::PRIORITY, $priority);

		ksort($node);

		$allData = array();

		foreach ($node as $prior => $data) {
			if ($prior === FileJournal::INFO) {
				continue;
			} elseif ($prior > $priority) {
				break;
			}

			$this->arrayAppendKeys($allData, $this->mergeIndexData($data));
		}

		$nodeInfo = $node[FileJournal::INFO];
		while ($nodeInfo[FileJournal::PREV_NODE] !== -1) {
			$nodeId = $nodeInfo[FileJournal::PREV_NODE];
			$node = $this->getNode($nodeId);

			if ($node === FALSE) {
				if (self::$debug) {
					throw new Nette\InvalidStateException("Cannot load node number $nodeId.");
				}
				break;
			}

			$nodeInfo = $node[FileJournal::INFO];
			unset($node[FileJournal::INFO]);

			foreach ($node as $data) {
				$this->arrayAppendKeys($allData, $this->mergeIndexData($data));
			}
		}

		return $this->cleanLinks($allData, $toDelete);
	}


	/**
	 * Cleans links from $data.
	 * @param  array
	 * @param  array
	 * @return array of removed items
	 */
	protected function cleanLinks(array $data, array & $toDelete)
	{
		$return = array();

		$data = array_keys($data);
		sort($data);
		$max = count($data);
		$data[] = 0;
		$i = 0;

		while ($i < $max) {
			$searchLink = $data[$i];

			if (isset($this->deletedLinks[$searchLink])) {
				++$i;
				continue;
			}

			$nodeId = $searchLink >> FileJournal::BITROT;
			$node = $this->getNode($nodeId);

			if ($node === FALSE) {
				if (self::$debug) {
					throw new Nette\InvalidStateException("Cannot load node number $nodeId.");
				}
				++$i;
				continue;
			}

			do {
				$link = $data[$i];

				if (!isset($node[$link])) {
					if (self::$debug) {
						throw new Nette\InvalidStateException("Link with ID $searchLink is not in node $nodeId.");
					}
					continue;
				} elseif (isset($this->deletedLinks[$link])) {
					continue;
				}

				$nodeLink = & $node[$link];
				if (!$nodeLink[FileJournal::DELETED]) {
					$nodeLink[FileJournal::DELETED] = TRUE;
					$return[] = $nodeLink[FileJournal::KEY];
				} else {
					foreach ($nodeLink[FileJournal::TAGS] as $tag) {
						$toDelete[FileJournal::TAGS][$tag][$link] = TRUE;
					}
					if ($nodeLink[FileJournal::PRIORITY] !== FALSE) {
						$toDelete[FileJournal::PRIORITY][$nodeLink[FileJournal::PRIORITY]][$link] = TRUE;
					}
					$toDelete[FileJournal::ENTRIES][crc32($nodeLink[FileJournal::KEY])][$link] = TRUE;
					unset($node[$link]);
					$this->deletedLinks[$link] = TRUE;
				}
			} while (($data[++$i] >> FileJournal::BITROT) === $nodeId);

			$this->saveNode($nodeId, $node);
		}

		return $return;
	}


	/**
	 * Remove links to deleted keys from index.
	 * @param  array
	 */
	protected function cleanFromIndex(array $toDeleteFromIndex)
	{
		foreach ($toDeleteFromIndex as $type => $toDelete) {
			ksort($toDelete);

			while (!empty($toDelete)) {
				reset($toDelete);
				$searchKey = key($toDelete);
				list($masterNodeId, $masterNode) = $this->findIndexNode($type, $searchKey);

				if (!isset($masterNode[$searchKey])) {
					if (self::$debug) {
						throw new Nette\InvalidStateException('Bad index.');
					}
					unset($toDelete[$searchKey]);
					continue;
				}

				foreach ($toDelete as $key => $links) {
					if (isset($masterNode[$key])) {
						foreach ($links as $link => $foo) {
							if (isset($masterNode[$key][$link])) {
								unset($masterNode[$key][$link], $links[$link]);
							}
						}

						if (!empty($links) && isset($masterNode[$key][FileJournal::INDEX_DATA])) {
							$this->cleanIndexData($masterNode[$key][FileJournal::INDEX_DATA], $links, $masterNode[$key]);
						}

						if (empty($masterNode[$key])) {
							unset($masterNode[$key]);
						}
						unset($toDelete[$key]);
					} else {
						break;
					}
				}
				$this->saveNode($masterNodeId, $masterNode);
			}
		}
	}


	/**
	 * Merge data with index data in other nodes.
	 * @param  array
	 * @return array of merged items
	 */
	protected function mergeIndexData(array $data)
	{
		while (isset($data[FileJournal::INDEX_DATA])) {
			$id = $data[FileJournal::INDEX_DATA];
			unset($data[FileJournal::INDEX_DATA]);
			$childNode = $this->getNode($id);

			if ($childNode === FALSE) {
				if (self::$debug) {
					throw new Nette\InvalidStateException("Cannot load node number $id.");
				}
				break;
			}

			$this->arrayAppendKeys($data, $childNode[FileJournal::INDEX_DATA]);
		}

		return $data;
	}


	/**
	 * Cleans links from other nodes.
	 * @param  int
	 * @param  array
	 * @param  array
	 * @return void
	 */
	protected function cleanIndexData($nextNodeId, array $links, & $masterNodeLink)
	{
		$prev = -1;

		while ($nextNodeId && !empty($links)) {
			$nodeId = $nextNodeId;
			$node = $this->getNode($nodeId);

			if ($node === FALSE) {
				if (self::$debug) {
					throw new Nette\InvalidStateException("Cannot load node number $nodeId.");
				}
				break;
			}

			foreach ($links as $link => $foo) {
				if (isset($node[FileJournal::INDEX_DATA][$link])) {
					unset($node[FileJournal::INDEX_DATA][$link], $links[$link]);
				}
			}

			if (isset($node[FileJournal::INDEX_DATA][FileJournal::INDEX_DATA])) {
				$nextNodeId = $node[FileJournal::INDEX_DATA][FileJournal::INDEX_DATA];
			} else {
				$nextNodeId = FALSE;
			}

			if (empty($node[FileJournal::INDEX_DATA]) || (count($node[FileJournal::INDEX_DATA]) === 1 && $nextNodeId)) {
				if ($prev === -1) {
					if ($nextNodeId === FALSE) {
						unset($masterNodeLink[FileJournal::INDEX_DATA]);
					} else {
						$masterNodeLink[FileJournal::INDEX_DATA] = $nextNodeId;
					}
				} else {
					$prevNode = $this->getNode($prev);
					if ($prevNode === FALSE) {
						if (self::$debug) {
							throw new Nette\InvalidStateException("Cannot load node number $prev.");
						}
					} else {
						if ($nextNodeId === FALSE) {
							unset($prevNode[FileJournal::INDEX_DATA][FileJournal::INDEX_DATA]);
							if (empty($prevNode[FileJournal::INDEX_DATA])) {
								unset($prevNode[FileJournal::INDEX_DATA]);
							}
						} else {
							$prevNode[FileJournal::INDEX_DATA][FileJournal::INDEX_DATA] = $nextNodeId;
						}

						$this->saveNode($prev, $prevNode);
					}
				}
				unset($node[FileJournal::INDEX_DATA]);
			} else {
				$prev = $nodeId;
			}

			$this->saveNode($nodeId, $node);
		}
	}


	/**
	 * Get node from journal.
	 * @param  integer
	 * @return array
	 */
	protected function getNode($id)
	{
		// Load from cache
		if (isset($this->nodeCache[$id])) {
			return $this->nodeCache[$id];
		}

		$binary = stream_get_contents($this->handle, FileJournal::NODE_SIZE, FileJournal::HEADER_SIZE + FileJournal::NODE_SIZE * $id);

		if (empty($binary)) {
			// empty node, no Exception
			return FALSE;
		}

		list(, $magic, $length) = unpack('N2', $binary);
		if ($magic !== FileJournal::INDEX_MAGIC && $magic !== FileJournal::DATA_MAGIC) {
			if (!empty($magic)) {
				if (self::$debug) {
					throw new Nette\InvalidStateException("Node $id has malformed header.");
				}
				$this->deleteNode($id);
			}
			return FALSE;
		}

		$data = substr($binary, 2 * FileJournal::INT32_SIZE, $length - 2 * FileJournal::INT32_SIZE);

		$node = @unserialize($data); // intentionally @
		if ($node === FALSE) {
			$this->deleteNode($id);
			if (self::$debug) {
				throw new Nette\InvalidStateException("Cannot unserialize node number $id.");
			}
			return FALSE;
		}

		// Save to cache and return
		return $this->nodeCache[$id] = $node;
	}


	/**
	 * Save node to cache.
	 * @param  integer
	 * @param  array
	 * @return void
	 */
	protected function saveNode($id, array $node)
	{
		if (count($node) === 1) { // Nod contains only INFO
			$nodeInfo = $node[FileJournal::INFO];
			if ($nodeInfo[FileJournal::TYPE] !== FileJournal::DATA) {

				if ($nodeInfo[FileJournal::END] !== -1) {
					$this->nodeCache[$id] = $node;
					$this->nodeChanged[$id] = TRUE;
					return;
				}

				if ($nodeInfo[FileJournal::MAX] === -1) {
					$max = PHP_INT_MAX;
				} else {
					$max = $nodeInfo[FileJournal::MAX];
				}

				list(, , $parentId) = $this->findIndexNode($nodeInfo[FileJournal::TYPE], $max, $id);
				if ($parentId !== -1 && $parentId !== $id) {
					$parentNode = $this->getNode($parentId);
					if ($parentNode === FALSE) {
						if (self::$debug) {
							throw new Nette\InvalidStateException("Cannot load node number $parentId.");
						}
					} else {
						if ($parentNode[FileJournal::INFO][FileJournal::END] === $id) {
							if (count($parentNode) === 1) {
								$parentNode[FileJournal::INFO][FileJournal::END] = -1;
							} else {
								end($parentNode);
								$lastKey = key($parentNode);
								$parentNode[FileJournal::INFO][FileJournal::END] = $parentNode[$lastKey];
								unset($parentNode[$lastKey]);
							}
						} else {
							unset($parentNode[$nodeInfo[FileJournal::MAX]]);
						}

						$this->saveNode($parentId, $parentNode);
					}
				}

				if ($nodeInfo[FileJournal::TYPE] === FileJournal::PRIORITY) { // only priority tree has link to prevNode
					if ($nodeInfo[FileJournal::MAX] === -1) {
						if ($nodeInfo[FileJournal::PREV_NODE] !== -1) {
							$prevNode = $this->getNode($nodeInfo[FileJournal::PREV_NODE]);
							if ($prevNode === FALSE) {
								if (self::$debug) {
									throw new Nette\InvalidStateException("Cannot load node number {$nodeInfo[FileJournal::PREV_NODE]}.");
								}
							} else {
								$prevNode[FileJournal::INFO][FileJournal::MAX] = -1;
								$this->saveNode($nodeInfo[FileJournal::PREV_NODE], $prevNode);
							}
						}
					} else {
						list($nextId, $nextNode) = $this->findIndexNode($nodeInfo[FileJournal::TYPE], $nodeInfo[FileJournal::MAX] + 1, NULL, $id);
						if ($nextId !== -1 && $nextId !== $id) {
							$nextNode[FileJournal::INFO][FileJournal::PREV_NODE] = $nodeInfo[FileJournal::PREV_NODE];
							$this->saveNode($nextId, $nextNode);
						}
					}
				}
			}
			$this->nodeCache[$id] = FALSE;
		} else {
			$this->nodeCache[$id] = $node;
		}
		$this->nodeChanged[$id] = TRUE;
	}


	/**
	 * Commit all changed nodes from cache to journal file.
	 * @return void
	 */
	protected function commit()
	{
		do {
			foreach ($this->nodeChanged as $id => $foo) {
				if ($this->prepareNode($id, $this->nodeCache[$id])) {
					unset($this->nodeChanged[$id]);
				}
			}
		} while (!empty($this->nodeChanged));

		foreach ($this->toCommit as $node => $str) {
			$this->commitNode($node, $str);
		}
		$this->toCommit = array();
	}


	/**
	 * Prepare node to journal file structure.
	 * @param  integer
	 * @param  array|bool
	 * @return bool Successfully committed
	 */
	protected function prepareNode($id, $node)
	{
		if ($node === FALSE) {
			if ($id < $this->lastNode) {
				$this->lastNode = $id;
			}
			unset($this->nodeCache[$id]);
			unset($this->dataNodeFreeSpace[$id]);
			$this->deleteNode($id);
			return TRUE;
		}

		$data = serialize($node);
		$dataSize = strlen($data) + 2 * FileJournal::INT32_SIZE;

		$isData = $node[FileJournal::INFO][FileJournal::TYPE] === FileJournal::DATA;
		if ($dataSize > FileJournal::NODE_SIZE) {
			if ($isData) {
				throw new Nette\InvalidStateException('Saving node is bigger than maximum node size.');
			} else {
				$this->bisectNode($id, $node);
				return FALSE;
			}
		}

		$this->toCommit[$id] = pack('N2', $isData ? FileJournal::DATA_MAGIC : FileJournal::INDEX_MAGIC, $dataSize) . $data;

		if ($this->lastNode < $id) {
			$this->lastNode = $id;
		}
		if ($isData) {
			$this->dataNodeFreeSpace[$id] = FileJournal::NODE_SIZE - $dataSize;
		}

		return TRUE;
	}


	/**
	 * Commit node string to journal file.
	 * @param  integer
	 * @param  string
	 * @return void
	 */
	protected function commitNode($id, $str)
	{
		fseek($this->handle, FileJournal::HEADER_SIZE + FileJournal::NODE_SIZE * $id);
		$written = fwrite($this->handle, $str);
		if ($written === FALSE) {
			throw new Nette\InvalidStateException("Cannot write node number $id to journal.");
		}
	}


	/**
	 * Find right node in B+tree. .
	 * @param  string Tree type (TAGS, PRIORITY or ENTRIES)
	 * @param  int    Searched item
	 * @return array Node
	 */
	protected function findIndexNode($type, $search, $childId = NULL, $prevId = NULL)
	{
		$nodeId = self::$startNode[$type];

		$parentId = -1;
		while (TRUE) {
			$node = $this->getNode($nodeId);

			if ($node === FALSE) {
				return array(
					$nodeId,
					array(
						FileJournal::INFO => array(
							FileJournal::TYPE => $type,
							FileJournal::IS_LEAF => TRUE,
							FileJournal::PREV_NODE => -1,
							FileJournal::END => -1,
							FileJournal::MAX => -1,
						)
					),
					$parentId,
				); // Init empty node
			}

			if ($node[FileJournal::INFO][FileJournal::IS_LEAF] || $nodeId === $childId || $node[FileJournal::INFO][FileJournal::PREV_NODE] === $prevId) {
				return array($nodeId, $node, $parentId);
			}

			$parentId = $nodeId;

			if (isset($node[$search])) {
				$nodeId = $node[$search];
			} else {
				foreach ($node as $key => $childNode) {
					if ($key > $search and $key !== FileJournal::INFO) {
						$nodeId = $childNode;
						continue 2;
					}
				}

				$nodeId = $node[FileJournal::INFO][FileJournal::END];
			}
		}
	}


	/**
	 * Find complete free node.
	 * @param  integer
	 * @return array|integer Node ID
	 */
	protected function findFreeNode($count = 1)
	{
		$id = $this->lastNode;
		$nodesId = array();

		do {
			if (isset($this->nodeCache[$id])) {
				++$id;
				continue;
			}

			$offset = FileJournal::HEADER_SIZE + FileJournal::NODE_SIZE * $id;

			$binary = stream_get_contents($this->handle, FileJournal::INT32_SIZE, $offset);

			if (empty($binary)) {
				$nodesId[] = $id;
			} else {
				list(, $magic) = unpack('N', $binary);
				if ($magic !== FileJournal::INDEX_MAGIC && $magic !== FileJournal::DATA_MAGIC) {
					$nodesId[] = $id;
				}
			}

			++$id;
		} while (count($nodesId) !== $count);

		if ($count === 1) {
			return $nodesId[0];
		} else {
			return $nodesId;
		}
	}


	/**
	 * Find free data node that has $size bytes of free space.
	 * @param  integer size in bytes
	 * @return integer Node ID
	 */
	protected function findFreeDataNode($size)
	{
		foreach ($this->dataNodeFreeSpace as $id => $freeSpace) {
			if ($freeSpace > $size) {
				return $id;
			}
		}

		$id = self::$startNode[FileJournal::DATA];
		while (TRUE) {
			if (isset($this->dataNodeFreeSpace[$id]) || isset($this->nodeCache[$id])) {
				++$id;
				continue;
			}

			$offset = FileJournal::HEADER_SIZE + FileJournal::NODE_SIZE * $id;
			$binary = stream_get_contents($this->handle, 2 * FileJournal::INT32_SIZE, $offset);

			if (empty($binary)) {
				$this->dataNodeFreeSpace[$id] = FileJournal::NODE_SIZE;
				return $id;
			}

			list(, $magic, $nodeSize) = unpack('N2', $binary);
			if (empty($magic)) {
				$this->dataNodeFreeSpace[$id] = FileJournal::NODE_SIZE;
				return $id;
			} elseif ($magic === FileJournal::DATA_MAGIC) {
				$freeSpace = FileJournal::NODE_SIZE - $nodeSize;
				$this->dataNodeFreeSpace[$id] = $freeSpace;

				if ($freeSpace > $size) {
					return $id;
				}
			}

			++$id;
		}
	}


	/**
	 * Bisect node or when has only one key, move part to data node.
	 * @param  integer Node ID
	 * @param  array Node
	 * @return void
	 */
	protected function bisectNode($id, array $node)
	{
		$nodeInfo = $node[FileJournal::INFO];
		unset($node[FileJournal::INFO]);

		if (count($node) === 1) {
			$key = key($node);

			$dataId = $this->findFreeDataNode(FileJournal::NODE_SIZE);
			$this->saveNode($dataId, array(
				FileJournal::INDEX_DATA => $node[$key],
				FileJournal::INFO => array(
					FileJournal::TYPE => FileJournal::DATA,
					FileJournal::LAST_INDEX => ($dataId << FileJournal::BITROT),
			)));

			unset($node[$key]);
			$node[$key][FileJournal::INDEX_DATA] = $dataId;
			$node[FileJournal::INFO] = $nodeInfo;

			$this->saveNode($id, $node);
			return;
		}

		ksort($node);
		$halfCount = ceil(count($node) / 2);

		list($first, $second) = array_chunk($node, $halfCount, TRUE);

		end($first);
		$halfKey = key($first);

		if ($id <= 2) { // Root
			list($firstId, $secondId) = $this->findFreeNode(2);

			$first[FileJournal::INFO] = array(
				FileJournal::TYPE => $nodeInfo[FileJournal::TYPE],
				FileJournal::IS_LEAF => $nodeInfo[FileJournal::IS_LEAF],
				FileJournal::PREV_NODE => -1,
				FileJournal::END => -1,
				FileJournal::MAX => $halfKey,
			);
			$this->saveNode($firstId, $first);

			$second[FileJournal::INFO] = array(
				FileJournal::TYPE => $nodeInfo[FileJournal::TYPE],
				FileJournal::IS_LEAF => $nodeInfo[FileJournal::IS_LEAF],
				FileJournal::PREV_NODE => $firstId,
				FileJournal::END => $nodeInfo[FileJournal::END],
				FileJournal::MAX => -1,
			);
			$this->saveNode($secondId, $second);

			$parentNode = array(
				FileJournal::INFO => array(
					FileJournal::TYPE => $nodeInfo[FileJournal::TYPE],
					FileJournal::IS_LEAF => FALSE,
					FileJournal::PREV_NODE => -1,
					FileJournal::END => $secondId,
					FileJournal::MAX => -1,
				),
				$halfKey => $firstId,
			);
			$this->saveNode($id, $parentNode);
		} else {
			$firstId = $this->findFreeNode();

			$first[FileJournal::INFO] = array(
				FileJournal::TYPE => $nodeInfo[FileJournal::TYPE],
				FileJournal::IS_LEAF => $nodeInfo[FileJournal::IS_LEAF],
				FileJournal::PREV_NODE => $nodeInfo[FileJournal::PREV_NODE],
				FileJournal::END => -1,
				FileJournal::MAX => $halfKey,
			);
			$this->saveNode($firstId, $first);

			$second[FileJournal::INFO] = array(
				FileJournal::TYPE => $nodeInfo[FileJournal::TYPE],
				FileJournal::IS_LEAF => $nodeInfo[FileJournal::IS_LEAF],
				FileJournal::PREV_NODE => $firstId,
				FileJournal::END => $nodeInfo[FileJournal::END],
				FileJournal::MAX => $nodeInfo[FileJournal::MAX],
			);
			$this->saveNode($id, $second);

			list(,, $parent) = $this->findIndexNode($nodeInfo[FileJournal::TYPE], $halfKey);
			$parentNode = $this->getNode($parent);
			if ($parentNode === FALSE) {
				if (self::$debug) {
					throw new Nette\InvalidStateException("Cannot load node number $parent.");
				}
			} else {
				$parentNode[$halfKey] = $firstId;
				ksort($parentNode); // Parent index must be always sorted.
				$this->saveNode($parent, $parentNode);
			}
		}
	}


	/**
	 * Commit header to journal file.
	 * @return void
	 */
	protected function headerCommit()
	{
		fseek($this->handle, FileJournal::INT32_SIZE);
		@fwrite($this->handle, pack('N', $this->lastNode));  // intentionally @, save is not necessary
	}


	/**
	 * Remove node from journal file.
	 * @param  integer
	 * @return void
	 */
	protected function deleteNode($id)
	{
		fseek($this->handle, 0, SEEK_END);
		$end = ftell($this->handle);

		if ($end <= (FileJournal::HEADER_SIZE + FileJournal::NODE_SIZE * ($id + 1))) {
			$packedNull = pack('N', 0);

			do {
				$binary = stream_get_contents($this->handle, FileJournal::INT32_SIZE, (FileJournal::HEADER_SIZE + FileJournal::NODE_SIZE * --$id));
			} while (empty($binary) || $binary === $packedNull);

			if (!ftruncate($this->handle, FileJournal::HEADER_SIZE + FileJournal::NODE_SIZE * ($id + 1))) {
				throw new Nette\InvalidStateException('Cannot truncate journal file.');
			}
		} else {
			fseek($this->handle, FileJournal::HEADER_SIZE + FileJournal::NODE_SIZE * $id);
			$written = fwrite($this->handle, pack('N', 0));
			if ($written !== FileJournal::INT32_SIZE) {
				throw new Nette\InvalidStateException("Cannot delete node number $id from journal.");
			}
		}
	}


	/**
	 * Complete delete all nodes from file.
	 * @throws \Nette\InvalidStateException
	 */
	protected function deleteAll()
	{
		if (!ftruncate($this->handle, FileJournal::HEADER_SIZE)) {
			throw new Nette\InvalidStateException('Cannot truncate journal file.');
		}
	}


	/**
	 * Lock file for writing and reading and delete node cache when file has changed.
	 * @throws \Nette\InvalidStateException
	 */
	protected function lock()
	{
		if (!$this->handle) {
			$this->prepare();
		}

		if (!flock($this->handle, LOCK_EX)) {
			throw new Nette\InvalidStateException("Cannot acquire exclusive lock on journal file '$this->file'.");
		}

		$lastProcessIdentifier = stream_get_contents($this->handle, FileJournal::INT32_SIZE, FileJournal::INT32_SIZE * 2);
		if ($lastProcessIdentifier !== $this->processIdentifier) {
			$this->nodeCache = $this->dataNodeFreeSpace = array();

			// Write current processIdentifier to file header
			fseek($this->handle, FileJournal::INT32_SIZE * 2);
			fwrite($this->handle, $this->processIdentifier);
		}
	}


	/**
	 * Open btfj.dat file (or create it if not exists) and load metainformation
	 * @throws \Nette\InvalidStateException
	 */
	protected function prepare()
	{
		// Create journal file when not exists
		if (!file_exists($this->file)) {
			$init = @fopen($this->file, 'xb'); // intentionally @
			if (!$init) {
				clearstatcache();
				if (!file_exists($this->file)) {
					throw new Nette\InvalidStateException("Cannot create journal file '$this->file'.");
				}
			} else {
				$written = fwrite($init, pack('N2', FileJournal::FILE_MAGIC, $this->lastNode));
				fclose($init);

				$this->filePermissionsManager->fixOwnership($init);

				ManagedFileStorage::fixOwnership($init, $this->ownership); // change ownership of cache dir
				if ($written !== FileJournal::INT32_SIZE * 2) {
					throw new Nette\InvalidStateException("Cannot write journal header.");
				}
			}
		}

		$this->handle = fopen($this->file, 'r+b');

		if (!$this->handle) {
			throw new Nette\InvalidStateException("Cannot open journal file '$this->file'.");
		}

		if (!flock($this->handle, LOCK_SH)) {
			throw new Nette\InvalidStateException('Cannot acquire shared lock on journal.');
		}

		$header = stream_get_contents($this->handle, 2 * FileJournal::INT32_SIZE, 0);

		flock($this->handle, LOCK_UN);

		list(, $fileMagic, $this->lastNode) = unpack('N2', $header);

		if ($fileMagic !== FileJournal::FILE_MAGIC) {
			fclose($this->handle);
			$this->handle = FALSE;
			throw new Nette\InvalidStateException("Malformed journal file '$this->file'.");
		}

		$this->processIdentifier = pack('N', mt_rand());
	}


	/**
	 * Unlock file and save last modified time.
	 * @return void
	 */
	protected function unlock()
	{
		if ($this->handle) {
			fflush($this->handle);
			flock($this->handle, LOCK_UN);
		}
	}


	/**
	 * @param  int $nodeId
	 * @param  array $nodeData
	 * @return int
	 * @throws \Nette\InvalidStateException
	 */
	protected function findNextFreeKey($nodeId, array & $nodeData)
	{
		$newKey = $nodeData[FileJournal::INFO][FileJournal::LAST_INDEX] + 1;
		$maxKey = ($nodeId + 1) << FileJournal::BITROT;

		if ($newKey >= $maxKey) {
			$start = $nodeId << FileJournal::BITROT;
			for ($i = $start; $i < $maxKey; $i++) {
				if (!isset($nodeData[$i])) {
					return $i;
				}
			}
			throw new Nette\InvalidStateException("Node $nodeId is full.");
		} else {
			return ++$nodeData[FileJournal::INFO][FileJournal::LAST_INDEX];
		}
	}


	/**
	 * Append $append to $array.
	 * This function is much faster then $array = array_merge($array, $append)
	 * @param  array
	 * @param  array
	 * @return void
	 */
	protected function arrayAppend(array & $array, array $append)
	{
		foreach ($append as $value) {
			$array[] = $value;
		}
	}


	/**
	 * Append $append to $array with preserve keys.
	 * This function is much faster then $array = $array + $append
	 * @param  array
	 * @param  array
	 * @return void
	 */
	protected function arrayAppendKeys(array & $array, array $append)
	{
		foreach ($append as $key => $value) {
			$array[$key] = $value;
		}
	}
}
