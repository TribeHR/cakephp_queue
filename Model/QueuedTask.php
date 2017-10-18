<?php

/**
 * @author MGriesbach@gmail.com
 * @package QueuePlugin
 * @subpackage QueuePlugin.Models
 * @license http://www.opensource.org/licenses/mit-license.php The MIT License
 * @link http://github.com/MSeven/cakephp_queue
 */
class QueuedTask extends AppModel {

	var $useDbConfig = 'central';
	
	public $name = 'QueuedTask';
	
	public $rateHistory = array();
	
	public $exit = false;
	
	public $_findMethods = array(
		'progress' => true
	);

	/**
	 * Add a new Job to the Queue
	 *
	 * @param string $jobName QueueTask name
	 * @param array $data any array
	 * @param string $notBefore An interval string ('+5 seconds', etc) to delay the job's execution by
	 * @param string $group Used to group similar QueuedTasks
	 * @param string $reference any array
	 * @return bool success
	 */
	public function createJob($jobName, $data, $notBefore = null, $group = 'default', $reference = null) {
		
		$data = array(
			'jobtype' => $jobName,
			'data' => serialize($data),
			'group' => $group,
			'reference' => $reference
		);
		if ($notBefore != null) {
			$data['notbefore'] = date('Y-m-d H:i:s', strtotime($notBefore));
		}
		$createdData = $this->create($data);
		if (defined('EMAIL_LOG_ENABLED_VALUE') && EMAIL_LOG_ENABLED_VALUE == 2) {
			if (in_array($jobName, array('Notification', 'NotificationEmail', 'Email'))) {
				$data = unserialize($createdData['QueuedTask']['data']);
				$Shell = new Shell();
				$tasks = new TaskCollection($Shell);

				$task = $tasks->load('queue' . $jobName);
				$task->run($data);

				return true;
			}
		}

		return $this->save($createdData);
	}

	/**
	 * Add a new Job to the Queue, unless an identical Job is awaiting execution.
	 *
	 * @param string $jobName QueueTask name
	 * @param array $data any array
	 * @param string $notBefore An interval string ('+5 seconds', etc) to delay the job's execution by
	 * @param string $group Used to group similar QueuedTasks
	 * @param string $reference any array
	 * @return bool success
	 */
	public function createSingletonJob($jobName, $data, $notBefore = null, $group = 'default', $reference = null) {
		// A job is already pending iff:
		// - it is of the same type and group
		// - it has the exact same data signature
		// - it has not been started/fetched
		//       (any job that has been fetched is either finished, in progress with potentially stale data,
		//       or in an error-handling state - queue a new one either way)
		$pendingInstances = $this->find('count', array(
			'recursive' => -1,
			'conditions' => array(
				'jobtype' => $jobName,
				'group' => $group,
				'data' => serialize($data),
				'fetched' => null
			)));

		// If an identical job is awaiting execution, don't add this one
		if ($pendingInstances > 0) {
			return true;
		}

		return ($this->createJob($jobName, $data, $notBefore, $group, $reference));
	}

	public function onError() {
		$this->exit = true;
	}

	/**
	 * Look for a new job that can be processed with the current abilities and
	 * from the specified group (or any if null).
	 *
	 * @param array $capabilities Available QueueWorkerTasks.
	 * @param string $group Request a job from this group, (from any group if null)
	 * @return Array Taskdata.
	 */
	public function requestJob($capabilities, $group = 'default') {
		$idlist = array();
		$wasFetched = array();
		
		$findConf = array(
			'conditions' => array(
				'completed' => null,
				'OR' => array()
			),
			'fields' => array(
				'id',
				'fetched',
				'timediff(NOW(),notbefore) AS age'
			),
			'order' => array(
				'age DESC',
				'id ASC'
			),
			'limit' => 3
		);
		
		if (!is_null($group)) {
			$findConf['conditions']['group'] = $group;
		}
		
		// generate the task specific conditions.
		foreach ($capabilities as $task) {
			$tmp = array(
				'jobtype' => str_replace('queue', '', $task['name']),
				'AND' => array(
					array(
						'OR' => array(
							'notbefore <' => date('Y-m-d H:i:s'),
							'notbefore' => null
						)
					),
					array(
						'OR' => array(
							'fetched <' => date('Y-m-d H:i:s', time() - $task['timeout']),
							'fetched' => null
						)
					)
				),
				'failed <' => ($task['retries'] + 1)
			);
			if (array_key_exists('rate', $task) && array_key_exists($tmp['jobtype'], $this->rateHistory)) {
				$tmp['NOW() >='] = date('Y-m-d H:i:s', $this->rateHistory[$tmp['jobtype']] + $task['rate']);
			}
			$findConf['conditions']['OR'][] = $tmp;
		}
		// First, find a list of a few of the oldest unfinished tasks.
		$data = $this->find('all', $findConf);

		if (is_array($data) && count($data) > 0) {
			// generate a list of their ID's
			foreach ($data as $item) {
				$idlist[] = $item[$this->name]['id'];
				if (!empty($item[$this->name]['fetched'])) {
					$wasFetched[] = $item[$this->name]['id'];
				}
			}
			// Generate a unique Identifier for the current worker thread
			$key = sha1(microtime());
			// try to update one of the found tasks with the key of this worker.
			$this->query('UPDATE ' . $this->tablePrefix . $this->table . ' SET workerkey = "' . $key . '", fetched = "' . date('Y-m-d H:i:s') . '" WHERE id in(' . implode(',', $idlist) . ') AND (workerkey IS NULL OR     fetched <= "' . date('Y-m-d H:i:s', time() - $task['timeout']) . '") ORDER BY timediff(NOW(),notbefore) DESC LIMIT 1');
			// read which one actually got updated, which is the job we are supposed to execute.
			$data = $this->find('first', array(
				'conditions' => array(
					'workerkey' => $key
				)
			));
			if (is_array($data) && !empty($data)) {
				// if the job had an existing fetched timestamp, increment the failure counter
				if (in_array($data[$this->name]['id'], $wasFetched)) {
					$data[$this->name]['failed']++;
					$data[$this->name]['failure_message'] = 'Restart after timeout';
					$this->save($data);
				}
				//save last fetch by type for Rate Limiting.
				$this->rateHistory[$data[$this->name]['jobtype']] = time();
				return $data[$this->name];
			}
		}
		return FALSE;
	}

	/**
	 * Mark a job as Completed, removing it from the queue.
	 *
	 * @param integer $id
	 * @return bool Success
	 */
	public function markJobDone($id) {
		return ($this->updateAll(array(
			'completed' => "'" . date('Y-m-d H:i:s') . "'"
		), array(
			'id' => $id
		)));
	}

	/**
	 * Mark a job as Failed, Incrementing the failed-counter and Requeueing it.
	 *
	 * @param integer $id
	 * @param string $failureMessage Optional message to append to the
	 * failure_message field
	 */
	public function markJobFailed($id, $failureMessage = null) {
		$db =& $this->getDataSource();
		return ($this->updateAll(array(
			'failed' => "failed + 1",
			'failure_message' => $db->value($failureMessage, 'failure_message')
		), array(
			'id' => $id
		)));
	}

	/**
	 * Requeue a job without incrementing its failure count. Used when a job could not be attempted.
	 *
	 * @param integer $id
	 * @param integer $timeout Number of seconds to wait before trying this job again.
	 */
	public function requeueJob($id, $timeout) {
		$db =& $this->getDataSource();
		return ($this->updateAll(array(
			'fetched' => null,
			'workerkey' => null,
			'notbefore' => "'". date('Y-m-d H:i:s', time() + $timeout) ."'"
		), array(
			'id' => $id
		)));
	}

	/**
	 * Returns the number of items in the Queue.
	 * Either returns the number of ALL pending tasks, or the number of pending tasks of the passed Type
	 *
	 * @param string $type jobType to Count
	 * @return integer
	 */
	public function getLength($type = null) {
		$findConf = array(
			'conditions' => array(
				'completed' => null
			)
		);
		if ($type != NULL) {
			$findConf['conditions']['jobtype'] = $type;
		}

		return $this->find('count', $findConf);
	}

	/**
	 * Returns the number of items in the Queue.
	 * Either returns the number of ALL pending tasks, or the number of pending tasks of the passed Type
	 *
	 * @param string $type jobType to Count
	 * @param boolean $omitScheduled Whether to count jobs that are waiting for a not_before time to execute.
	 * @return integer
	 */
	public function getPending() {
		$findConf = array(
			'conditions' => array(
				'completed' => null,
				'fetched' => null,
				'notbefore' => null,
			)
		);

		return $this->find('count', $findConf);
	}

	/**
	 * Return a list of all jobtypes in the Queue.
	 *
	 * @return array
	 */
	public function getTypes() {
		$findConf = array(
			'fields' => array(
				'jobtype'
			),
			'group' => array(
				'jobtype'
			)
		);
		return $this->find('list', $findConf);
	}

	/**
	 * Return some statistics about finished jobs still in the Database.
	 * @return array
	 */
	public function getStats() {
		$findConf = array(
			'fields' => array(
				'jobtype,count(id) as num, AVG(UNIX_TIMESTAMP(completed)-UNIX_TIMESTAMP(created)) AS alltime, AVG(UNIX_TIMESTAMP(completed)-UNIX_TIMESTAMP(fetched)) AS runtime, AVG(UNIX_TIMESTAMP(fetched)-IF(notbefore is null,UNIX_TIMESTAMP(created),UNIX_TIMESTAMP(notbefore))) AS fetchdelay'
			),
			'conditions' => array(
				'completed NOT' => null
			),
			'group' => array(
				'jobtype'
			)
		);
		return $this->find('all', $findConf);
	}

	/**
	 * Cleanup/Delete Completed Jobs.
	 *
	 */
	public function cleanOldJobs() {
		$this->deleteAll(array(
			'completed < ' => date('Y-m-d H:i:s', time() - Configure::read('queue.cleanuptimeout'))
		));
	
	}

	protected function _findProgress($state, $query = array(), $results = array()) {
		if ($state == 'before') {
			
			$query['fields'] = array(
				$this->alias . '.reference',
				'(CASE WHEN ' . $this->alias . '.notbefore > NOW() THEN \'NOT_READY\' WHEN ' . $this->alias . '.fetched IS NULL THEN \'NOT_STARTED\' WHEN ' . $this->alias . '.fetched IS NOT NULL AND ' . $this->alias . '.completed IS NULL AND ' . $this->alias . '.failed = 0 THEN \'IN_PROGRESS\' WHEN ' . $this->alias . '.fetched IS NOT NULL AND ' . $this->alias . '.completed IS NULL AND ' . $this->alias . '.failed > 0 THEN \'FAILED\' WHEN ' . $this->alias . '.fetched IS NOT NULL AND ' . $this->alias . '.completed IS NOT NULL THEN \'COMPLETED\' ELSE \'UNKNOWN\' END) AS status',
				$this->alias . '.failure_message'
			);
			if (isset($query['conditions']['exclude'])) {
				$exclude = $query['conditions']['exclude'];
				unset($query['conditions']['exclude']);
				$exclude = trim($exclude, ',');
				$exclude = explode(',', $exclude);
				$query['conditions'][] = array(
					'NOT' => array(
						'reference' => $exclude
					)
				);
			}
			if (isset($query['conditions']['group'])) {
				$query['conditions'][][$this->alias . '.group'] = $query['conditions']['group'];
				unset($query['conditions']['group']);
			}
			return $query;
		} else {
			foreach ($results as $k => $result) {
				$results[$k] = array(
					'reference' => $result[$this->alias]['reference'],
					'status' => $result[0]['status']
				);
				if (!empty($result[$this->alias]['failure_message'])) {
					$results[$k]['failure_message'] = $result[$this->alias]['failure_message'];
				}
			}
			return $results;
		}
	}

	public function clearDoublettes() {
		$x = $this->query('SELECT max(id) as id FROM `queueman`.`queued_tasks`
    where completed is null
    group by data
    having count(id) > 1');
		
		$start = 0;
		$x = array_keys($x);
		while ($start <= count($x)) {
			debug($this->deleteAll(array(
				'id' => array_slice($x, $start, 10)
			)));
			debug(array_slice($x, $start, 10));
			$start = $start + 100;
		}
	
	}

	/**
	 * Get the most recently completed task.
	 *
	 * @return array A QueuedTask 
	 */
	public function getLastCompleted() {

		$query = array(
			'conditions' => array(
				'completed IS NOT NULL',
				),
			'order' => array(
				'completed' => 'desc',
				),
			);

		return $this->find('first', $query);

	}
}

