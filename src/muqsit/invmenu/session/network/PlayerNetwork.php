<?php

declare(strict_types=1);

namespace muqsit\invmenu\session\network;

use Closure;
use muqsit\invmenu\InvMenuHandler;
use muqsit\invmenu\session\network\handler\PlayerNetworkHandler;
use pocketmine\network\mcpe\NetworkSession;
use pocketmine\network\mcpe\protocol\ProtocolInfo;
use pocketmine\network\mcpe\protocol\NetworkStackLatencyPacket;
use pocketmine\scheduler\ClosureTask;
use pocketmine\scheduler\TaskHandler;
use SplQueue;

final class PlayerNetwork{

	public const DELAY_TYPE_ANIMATION_WAIT = 0;
	public const DELAY_TYPE_OPERATION = 1;
	private const LEGACY_ACK_TIMEOUT_TICKS = 20;

	private ?NetworkStackLatencyEntry $current = null;
	private ?TaskHandler $legacy_ack_timeout = null;

	/** @var SplQueue<NetworkStackLatencyEntry> */
	private SplQueue $queue;

	public function __construct(
		readonly private NetworkSession $network_session,
		readonly private PlayerNetworkHandler $handler
	){
		$this->queue = new SplQueue();
	}

	public function finalize() : void{
		$this->dropPending();
	}

	public function dropPending() : void{
		$this->legacy_ack_timeout?->cancel();
		$this->legacy_ack_timeout = null;
		foreach($this->queue as $entry){
			($entry->then)(false);
		}
		$this->queue = new SplQueue();
		$this->setCurrent(null);
	}

	/**
	 * @param self::DELAY_TYPE_* $type
	 * @param Closure(bool) : bool $then
	 */
	public function wait(int $type, Closure $then) : void{
		$entry = $this->handler->createNetworkStackLatencyEntry($then, $this->network_session->getProtocolId());
		if($this->current !== null){
			$this->queue->enqueue($entry);
		}else{
			$this->setCurrent($entry);
		}
	}

	/**
	 * Waits at least $wait_ms before calling $then(true).
	 *
	 * @param self::DELAY_TYPE_* $type
	 * @param int $wait_ms
	 * @param Closure(bool) : bool $then
	 */
	public function waitUntil(int $type, int $wait_ms, Closure $then) : void{
		if($wait_ms <= 0 && $this->queue->isEmpty()){
			$then(true);
			return;
		}

		$elapsed_ms = 0.0;
		$this->wait($type, function(bool $success) use($wait_ms, $then, &$elapsed_ms) : bool{
			if($this->current === null){
				$then(false);
				return false;
			}

			$elapsed_ms += (microtime(true) * 1000) - $this->current->sent_at;
			if(!$success || $elapsed_ms >= $wait_ms){
				$then($success);
				return false;
			}

			return true;
		});
	}

	private function setCurrent(?NetworkStackLatencyEntry $entry) : void{
		$this->legacy_ack_timeout?->cancel();
		$this->legacy_ack_timeout = null;

		if($this->current !== null){
			$this->processCurrent(false);
		}

		$this->current = $entry;
		if($entry !== null){
			if($this->network_session->sendDataPacket(NetworkStackLatencyPacket::create($entry->network_timestamp, true))){
				$entry->sent_at = microtime(true) * 1000;
				if($this->network_session->getProtocolId() < ProtocolInfo::PROTOCOL_1_20_0){
					$this->legacy_ack_timeout = InvMenuHandler::getRegistrant()->getScheduler()->scheduleDelayedTask(new ClosureTask(function() use($entry) : void{
						// Legacy clients can occasionally miss latency ACKs. Don't let one missed ACK permanently stall the queue.
						if($this->current === $entry){
							$this->processCurrent(true);
						}
					}), self::LEGACY_ACK_TIMEOUT_TICKS);
				}
			}else{
				$this->processCurrent(false);
			}
		}
	}

	private function processCurrent(bool $success) : void{
		if($this->current !== null){
			$this->legacy_ack_timeout?->cancel();
			$this->legacy_ack_timeout = null;

			$current = $this->current;
			$repeat = ($current->then)($success);
			$this->current = null;
			if($repeat && $success){
				$this->setCurrent($current);
			}elseif(!$this->queue->isEmpty()){
				$this->setCurrent($this->queue->dequeue());
			}
		}
	}

	public function notify(int $timestamp) : void{
		if($this->current !== null && $timestamp === $this->current->timestamp){
			$this->processCurrent(true);
		}
	}
}
