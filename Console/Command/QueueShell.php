<?php
declare(ticks = 1);

/**
 * @author MGriesbach@gmail.com
 * @package QueuePlugin
 * @subpackage QueuePlugin.Shells
 * @license http://www.opensource.org/licenses/mit-license.php The MIT License
 * @link http://github.com/MSeven/cakephp_queue
 */

App::uses('Folder', 'Utility');
App::uses('File', 'Utility');
if (!class_exists('HipChat\HipChat')) {
	App::import('Vendor', 'Hipchat', array('file' => 'HipchatPhp/src/HipChat/HipChat.php')); 
}

class QueueShell extends Shell {
	public $uses = array(
		'CakephpQueue.QueuedTask'
	);
	/**
	 * Codecomplete Hint
	 *
	 * @var QueuedTask
	 */
	public $QueuedTask;
	
	private $taskConf;

	protected $_verbose = false;

	private $exit;

	const ALERT_SIZE = 100;
	const ALERT_COOLDOWN = 1800;


	function getOptionParser() {
		$parser = parent::getOptionParser();

		$parser->addOption(
			'group',
			array(
				'short' => 'g',
				'help' => 'Group for this queue to process.',
				'required' => false
				)
			);

		return $parser;
	}

	/**
	 * Overwrite shell initialize to dynamically load all Queue Related Tasks.
	 */
	public function initialize() {
		App::import('Folder');
		
		foreach (App::path('shells') as $path) {
			$folder = new Folder($path . DS . 'Task');
			$this->tasks = array_merge($this->tasks, $folder->find('queue.*\.php'));
		}
		// strip the extension fom the found task(file)s
		foreach ($this->tasks as &$task) {
			$task = basename($task, 'Task.php');
		}
		
		//Config can be overwritten via local app config.
		Configure::load('queue');
		
		$conf = Configure::read('queue');
		if (!is_array($conf)) {
			$conf = array();
		}
		//merge with default configuration vars.
		Configure::write('queue', array_merge(array(
			'sleeptime' => 10,
			'gcprop' => 10,
			'defaultworkertimeout' => 120,
			'defaultworkerretries' => 4,
			'workermaxruntime' => 0,
			'cleanuptimeout' => 2000,
			'exitwhennothingtodo' => false
		), $conf));

		if(isset($this->params['-verbose'])) {
			$this->_verbose = true;
		}

		parent::initialize();
	}

	/**
	 * Output some basic usage Info.
	 */
	public function help() {
		$this->out('CakePHP Queue Plugin:');
		$this->hr();
		$this->out('Information goes here.');
		$this->hr();
		$this->out('Usage: cake queue <command> <arg1> <arg2>...');
		$this->hr();
		$this->out('Commands:');
		$this->out('	queue help');
		$this->out('		shows this help message.', 2);
		$this->out('	queue add <taskname>');
		$this->out('		tries to call the cli `add()` function on a task.');
		$this->out('		tasks may or may not provide this functionality.', 2);
		$this->out('	cake queue runworker [--verbose]');
		$this->out('		run a queue worker, which will look for a pending task it can execute.');
		$this->out('		the worker will always try to find jobs matching its installed tasks.');
		$this->out('		see "Available tasks" below.', 2);
		$this->out('	queue stats');
		$this->out('		display some general statistics.', 2);
		$this->out('	queue clean');
		$this->out('		manually call cleanup function to delete task data of completed tasks.', 2);
		$this->out('Note:');
		$this->out('	<taskname> may either be the complete classname (eg. `queue_example`)');
		$this->out('	or the shorthand without the leading "queue_" (eg. `example`).', 2);
		$this->_listTasks();
	}

	/**
	 * Look for a Queue Task of hte passed name and try to call add() on it.
	 * A QueueTask may provide an add function to enable the user to create new jobs via commandline.
	 *
	 */
	public function add() {
		if (count($this->args) < 1) {
			$this->out('Usage:');
			$this->out('       cake queue add <taskname>', 2);
			$this->_listTasks();
		} else {
			if (in_array($this->args[0], $this->taskNames)) {
				$this->{$this->args[0]}->add();
			} elseif (in_array('queue' . $this->args[0], $this->taskNames)) {
				$this->{'queue' . $this->args[0]}->add();
			} else {
				$this->out('Error:');
				$this->out('       Task not found: ' . $this->args[0], 2);
				$this->_listTasks();
			}
		}
	}

	/**
	 * Run a QueueWorker loop.
	 * Runs a Queue Worker process which will try to find unassigned jobs in the queue
	 * which it may run and try to fetch and execute them.
	 */
	public function runworker() {
		// Enable Garbage Collector (PHP >= 5.3)
		if (function_exists('gc_enable')) {
		    gc_enable();
		}

		// Prevent mid-job termination (if platform supported)
		if (function_exists('pcntl_signal')) {
			pcntl_signal(SIGTERM, array(&$this, "_exit"));
			$this->exit = false;
		}

		$starttime = time();
		$group = null;
		if (isset($this->params['group']) && !empty($this->params['group'])) {
			$group = $this->params['group'];
		}
		while (!$this->exit) {
			if($this->_verbose) {
				$this->out('Looking for Job....');
			}
			$data = $this->QueuedTask->requestJob($this->getTaskConf(), $group);
			if ($this->QueuedTask->exit === true) {
				$this->exit = true;
			} else {
				if ($data !== false) {
					$this->out('Running Job of type "' . $data['jobtype'] . '"');
					$taskname = 'queue' . $data['jobtype'];
					$jobData = unserialize($data['data']);
					if (!$this->{$taskname}->canRun($jobData)) {
						$this->QueuedTask->requeueJob($data['id'], $this->getTaskConf($taskname, 'timeout'));
						$this->out('Job could not be run, requeued.');
					} else {
						$return = $this->{$taskname}->run($jobData);
						if ($return == true) {
							$this->QueuedTask->markJobDone($data['id']);
							$this->out('Job Finished.');
						} else {
							$failureMessage = null;
							if (isset($this->{$taskname}->failureMessage) && !empty($this->{$taskname}->failureMessage)) {
								$failureMessage = $this->{$taskname}->failureMessage;
							}
							$this->QueuedTask->markJobFailed($data['id'], $failureMessage);
							$this->out('Job did not finish, requeued.');
						}
					}
				} elseif (Configure::read('queue.exitwhennothingtodo')) {
					$this->out('nothing to do, exiting.');
					$this->exit = true;
				} else {
					if($this->_verbose) {
						$this->out('nothing to do, sleeping.');
					}
					sleep(Configure::read('queue.sleeptime'));
				}
				
				// check if we are over the maximum runtime and end processing if so.
				if (Configure::read('queue.workermaxruntime') != 0 && (time() - $starttime) >= Configure::read('queue.workermaxruntime')) {
					$this->exit = true;
					$this->out('Reached runtime of ' . (time() - $starttime) . ' Seconds (Max ' . Configure::read('queue.workermaxruntime') . '), terminating.');
				}
				if ($this->exit || rand(0, 100) > (100 - Configure::read('queue.gcprop'))) {
					$this->out('Performing Old job cleanup.');
					$this->QueuedTask->cleanOldJobs();
				}
				if($this->_verbose) {
					$this->hr();
				}
			}
		}
	}

	/**
	 * Manually trigger a Finished job cleanup.
	 * @return null
	 */
	public function clean() {
		$this->out('Deleting old jobs, that have finished before ' . date('Y-m-d H:i:s', time() - Configure::read('queue.cleanuptimeout')));
		$this->QueuedTask->cleanOldJobs();
	}

	/**
	 * Display Some statistics about Finished Jobs.
	 * @return null
	 */
	public function stats() {
		$this->out('Jobs currenty in the Queue:');
		
		$types = $this->QueuedTask->getTypes();
		
		foreach ($types as $type) {
			$this->out("      " . str_pad($type, 20, ' ', STR_PAD_RIGHT) . ": " . $this->QueuedTask->getLength($type));
		}
		$this->hr();
		$this->out('Total unfinished Jobs      : ' . $this->QueuedTask->getLength());
		$this->hr();
		$this->out('Finished Job Statistics:');
		$data = $this->QueuedTask->getStats();
		foreach ($data as $item) {
			$this->out(" " . $item['QueuedTask']['jobtype'] . ": ");
			$this->out("   Finished Jobs in Database: " . $item[0]['num']);
			$this->out("   Average Job existence    : " . $item[0]['alltime'] . 's');
			$this->out("   Average Execution delay  : " . $item[0]['fetchdelay'] . 's');
			$this->out("   Average Execution time   : " . $item[0]['runtime'] . 's');
		}
	}

	/**
	 * Write the status of the queue out to the terminal.
	 * @return null
	 */
	public function status() {
		$info = $this->getStatus();
		$this->out('Queue size: ' . $info['queue_size']);
		$this->out('Last Task Completed: ' . $info['last_task_completed']);
	}

	/**
	 * To be called periodically by a task scheduler like Cron. Writes the queue status to a file and 
	 * sends an alert if it meets certain warning conditions.
	 *
	 * @return null
	 */
	public function statusFile() {
		$info = $this->getStatus();
		if ($info['status'] == 'bad') {
			$this->sendAlert($info);
		}
		$this->writeFile($info);	
	}

	/**
	 * Fetch the status from the queue database and put data in an array the different output functions can use.
	 *
	 * @return array Keyed array of metrics we want from the queue.
	 */
	private function getStatus() {
		$info['queue_size'] = $this->QueuedTask->getPending();
		$lastTask = $this->QueuedTask->getLastCompleted();
		if (!empty($lastTask)) {
			$info['last_task_completed'] = $lastTask['QueuedTask']['completed'];
		} else {
			$info['last_task_completed'] = null;
		}
		$info['status'] = 'good';
		if ($info['queue_size'] > self::ALERT_SIZE) {
			$info['status'] = 'bad';
		}
		return $info;
	}

	/**
	 * Write the output of status() to a selected file path.
	 * Where the file is written is defined in app/Config/bootstrap.{environment}.php
	 *
	 * @param array $info The values to output
	 */
	private function writeFile($info) {
		App::import('File');
		$file = new File(QUEUE_MONITOR_OUTFILE);
		if (!$file->write('readJson(' . json_encode($info) . ")\n", $mode = 'w', $force = true)) {
			$this->out('Unable to write JSONP file.');
			CakeLog::error('CakephpQueue Monitor: Unable to write JSONP file.');
		}
		$file->close();
		$this->out('File written to: ' . QUEUE_MONITOR_OUTFILE);
	}
	
	/**
	 * Send Alert.  This could be an email or message to HipChat.
	 * For this version it goes to HipChat
	 * 
	 * @param array $info Array of queue info.
	 */
	private function sendAlert($info) {
		if ($this->spamCheck()) {
			$this->sendToHipchat(ENVIRONMENT . ' -- Queue has ' . $info['queue_size'] . ' pending items.');
			$this->out('Alert triggered.');
		}
	}

	/**
	 * Make sure we aren't sending the same message over and over.
	 *
	 * @return boolean true if we are not spamming, false if we do not want to send message.
	 */
	private function spamCheck() {
		App::import('File');
		$file = new File('tmp/queue_last_alert.txt');
		if ($file->exists()) {
			$last_alert = strtotime($file->read());
			if ((time() - $last_alert) < self::ALERT_COOLDOWN) {
				$this->out('Alert triggered, but on cooldown.');
				return false;
			}
		}
		if ($file->write(date('c'), $mode = 'w', $force = true)) {
			return true;
		}
		$this->out('Unable to write spam protection file.');
		CakeLog::error('CakephpQueue Monitor: Unable to write spam protection file.');
		// if we can't write to the file, we can't send the alert because we could be spamming.
		return false;
	}

	/**
	 * Sends the message to HipChat
	 *
	 * @param string The message you want to send
	 */
	private function sendToHipchat($message) {
		$hipchatToken = QUEUE_MONITOR_HIPCHAT_TOKEN;
		$hipchatRoomID = QUEUE_MONITOR_HIPCHAT_ROOM;
		$fromName = 'Queue Monitor';
		$notify = 1;
		$color = 'yellow';

		$hipchatConnection = new HipChat\HipChat($hipchatToken);
		try {
			$result = $hipchatConnection->message_room($hipchatRoomID, $fromName, $message, $notify, $color);
		} catch (HipChat\HipChat_Exception $e) {
			$this->out("Failed to alert to hipchat: " . $e->getMessage());
			$result = false;
		}
	}

	/**
	 * Returns a List of available QueueTasks and their individual configurations.
	 * @return array
	 */
	private function getTaskConf($taskname = null, $field = null) {
		if (!is_array($this->taskConf)) {
			$this->taskConf = array();
			foreach ($this->tasks as $task) {
				$this->taskConf[$task]['name'] = $task;
				if (property_exists($this->{$task}, 'timeout')) {
					$this->taskConf[$task]['timeout'] = $this->{$task}->timeout;
				} else {
					$this->taskConf[$task]['timeout'] = Configure::read('queue.defaultworkertimeout');
				}
				if (property_exists($this->{$task}, 'retries')) {
					$this->taskConf[$task]['retries'] = $this->{$task}->retries;
				} else {
					$this->taskConf[$task]['retries'] = Configure::read('queue.defaultworkerretries');
				}
				if (property_exists($this->{$task}, 'rate')) {
					$this->taskConf[$task]['rate'] = $this->{$task}->rate;
				}
			}
		}
		if (is_null($taskname)) {
			return $this->taskConf;
		}
		if (is_null($field)) {
			return $this->taskConf[$taskname];
		}
		return $this->taskConf[$taskname][$field];
	}
/**
 * Output a list of available tasks.
 */
	protected function _listTasks() {
		$this->out('Available tasks:');
		foreach ($this->taskNames as $loadedTask) {
			$this->out('	- ' . $loadedTask);
		}
	}
	
	function out($str = null, $newlines = 1, $level = Shell::NORMAL) {
		if ($newlines > 0) {
			$str = date('Y-m-d H:i:s').' '.$str;
		}
		return parent::out($str, $newlines, $level);
	}

	function _exit($signal) {
		$this->exit = true;
	}

}

