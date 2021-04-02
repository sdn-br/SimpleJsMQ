/**
 * SimpleJSMQ
 *
 * Simple Javascript Message Broker System
 *
 * @author Michael Virnstein
 * 
 * Copyright © 2021 Michael Virnstein
 */
let SimpleJsMQ = (function() {
	/**
	 * Class Exception
	 */
	class Exception {
		constructor(msg) {
			this.name = 'Exception';
			this.message = msg;
		};
	}

	/**
	 * Class ValueErrorException
	 */
	class ValueErrorException extends Exception {
		constructor(msg) {
			super(msg);
			this.name = 'ValueErrorException';
		};
	}

	/**
	 * Class DuplicateKeyException
	 */
	class DuplicateKeyException extends Exception {
		constructor(msg) {
			super(msg);
			this.name = 'DuplicateKeyException';
		};
	}

	/**
	 * Class NotFoundException
	 */
	class NotFoundException extends Exception {
		constructor(msg) {
			super(msg);
			this.name = 'NotFoundException';
		};
	}

	/**
	 * Class IllegalOperationException
	 */
	class IllegalOperationException extends Exception {
		constructor(msg) {s
			super(msg);
			this.name = 'IllegalOperationException';
		};
	}

	/**
	 * Class Payload
	 */
	class Payload {
		static #lastId = 0;
		#id;
		#event;
		#type;
		#data;
		constructor(event, type, data) {
			if (!(event instanceof Event)) {
				throw new ValueErrorException('event is invalid');
			}
			if (typeof type != 'string' || type.trim().length === 0) {
				throw new ValueErrorException('type is invalid');
			} 
			
			this.#id = ++Payload.#lastId;
			this.#event = event;
			this.#type = type;
			this.#data = data;
		}
		
		getId() {
			return this.#id;
		};
		
		getType() {
			return this.#type;
		};
		
		getData() {
			return this.#data;
		};
		
		getObject() {
			return {
				id: this.#id,
				event: this.#event.getObject(),
				type: this.#type,
				data: this.#data
			}
		};
	}

	/**
	 * Class Event
	 */
	class Event {
		static #lastId = 0;
		#id;
		#topic;
		#name;
		#payload;
		
		constructor(topic, eventName, dataType, data) {
			if (!(topic instanceof Topic)) {
				throw new ValueErrorException('topic is invalid');
			}
			if (typeof eventName != 'string' || eventName.trim().length === 0) {
				throw new ValueErrorException('eventName is invalid');
			} 
			if (typeof dataType != 'string' || dataType.trim().length === 0) {
				throw new ValueErrorException('dataType is invalid');
			}
			if (data === undefined || data === null || typeof data == 'string' && data.trim().length === 0) {
				throw new ValueErrorException('data is invalid');
			}			 
			
			this.#id = ++Event.#lastId;	
			this.#topic = topic;
			this.#name = eventName;
			this.#payload = new Payload(this, dataType, data);
		};
		
		getId() {
			return this.#id;
		};
		
		getTopic() {
			return this.#topic;
		};
		
		getName() {
			return this.#name;
		};
		
		getPayload() {
			return this.#payload;
		};
		
		getObject() {
			return {
				id: this.#id,
				topic: this.#topic.getObject(),
				name: this.#name,
				payload: this.#payload.getObject()
			};
		};
		
	}

	/**
	 * Class Topic
	 */
	class Topic {
		static #lastId = 0;
		#id
		#name;
		#subscribers;
		#manager;
		
		constructor(name) {
			if (typeof name !== 'string' || name.trim().length === 0) {
				throw new ValueErrorException('name is invalid');
			} 
			this.#id = ++Topic.#lastId;
			this.#name = name;
			this.#subscribers = {};
		};
		
		/**
		 * get id
		 */ 
		getId() {
			return this.#id;
		};
		
		/**
		 * get name
		 */ 
		getName() {
			return this.#name;
		};
		
		/**
		 * get subscriber
		 */ 
		getSubscriber(subscriberName) {
			if (!this.isSubscribed(subscriberName)) {
				return;
			}
			return this.#subscribers[subscriberName];
		}
		
		/**
		 * get all subscribers
		 */ 
		getAllSubscribers() {
			return Object.assign({}, this.#subscribers);
		}
		
		/**
		 * creates a new event
		 */
		#createEvent(eventType, dataType, data) {
			return new Event(this, eventType, dataType, data);
		};
		
		/**
		 * set manager
		 */
		#setManager(manager) {
			if (manager !== undefined && !(manager instanceof TopicManager)) {
				throw new ValueErrorException('manager is invalid');
			}
			this.#manager = manager;
		}
		
		/**
		 * get Manger
		 */
		getManager() {
			return this.#manager;
		};
		
		/**
		 * is managed
		 */
		isManaged() {
			return this.getManager() !== undefined;
		};
		
		/**
		 * add to a manager
		 */
		addToManager(manager) {
			if (this.isManaged() && this.getManager() !== manager) {
				throw new IllegalOperationException('Already registered to a TopicManager');
			}
			let m = this.getManager();
			this.#setManager(manager);
			if (m === undefined) {
				manager.addTopic(this);
			}
		};
		
		/**
		 * remove from a manager
		 */
		removeFromManager() {
			let m = this.getManager();
			this.#setManager();
			if (m !== undefined) {
				m.removeTopic(this.getName());
			}
		}
		
		/**
		 * Get the name of the Topic
		 */
		getName() {
			return this.#name;
		};
		
		/**
		 * Publish sychronously
		 * @param type
		 * @param data
		 */
		publish (eventType, dataType, data) {
			this.publishEvent(this.#createEvent(eventType, dataType, data));
		};
		
		/**
		 * Publish sychronously
		 * @param event
		 */
		publishEvent(event) {
			if (!(event instanceof Event)) {
				throw new ValueErrorException('Object of type Event expected');
			}
			Object.values(this.#subscribers).forEach(subscriber => {
			   subscriber(event);
			});
		}
		
		/**
		 * Register event in the queue and publish asynchronously
		 * @param type
		 * @param data
		 */
		async register(eventType, dataType, data) {
			this.registerEvent(this.#createEvent(eventType, dataType, data), 1);
		};
		
		/**
		 * Register event in the queue and publish asynchronously
		 * @param event
		 */
		async registerEvent(event) {
			if (!(event instanceof Event)) {
				throw new ValueErrorException('Object of type Event expected');
			}
			setTimeout(this.publishEvent(event), 1);
		};
		
		/**
		 * Returns true iff the given object is in the list of observers
		 * @param subscriber
		 * @returns {boolean}
		 */
		isSubscribed(subscriberName) { 
			if (typeof subscriberName !== 'string' || subscriberName.trim().length === 0) {
				throw new ValueErrorException('subscriberName is invalid');
			}
			return this.#subscribers.hasOwnProperty(subscriberName); 
		};
		
		/**
		 * Adds the provided object to the observers of this observable object
		 * @param subscriber
		 * @param filter
		 */
		subscribe(subscriberName, callback) {
			if (typeof subscriberName !== 'string' || subscriberName.trim().length === 0) {
				throw new ValueErrorException('subscriberName is invalid');
			}
			if (typeof callback !== 'function') {
				throw new ValueErrorException('callback is not a function');
			}
			if (this.isSubscribed(subscriberName)){
				throw new DuplicateKeyException(`Subscriber with name '${subscriberName}' already exists`);
			}
			this.#subscribers[subscriberName] = callback;
		};

		/**
		 * Removes the provided objects from the observers (if it is among them)
		 * @param s
		 */
		unsubscribe(subscriberName) {
			if (this.isSubscribed(subscriberName)) {
				delete this.#subscribers[subscriberName];
			}
		};
		
		getObject() {
			let subscribers = Object.keys(this.#subscribers);
			return {
				id: this.#id,
				name: this.#name,
				subscriberCount: subscribers.length,
				subscribers: subscribers
			};
		};
	};

	/**
	 * Class MessageBroker
	 */
	class MessageBroker {
		static #lastId = 0;
		#id;
		#topics;
		#preSubscribers;
		#preSubInterval;
		
		constructor() {
			this.#id = ++MessageBroker.#lastId;
			this.#topics = {};
			this.#preSubscribers = {};
			this.#preSubInterval = undefined;
		};
		
		/**
		 * getId	
		 */
		getId() {
			return this.#id;
		};
		
		/**
		 * creates a new managed Topic
		 */
		createTopic(topicName) {
			let topic = new Topic(topicName);
			topic.addToManager(this);
			return topic;
		};
		
		/**
		 *  add a topic
		 *  @param topic
		 */
		addTopic(topic) {
			if (!(topic instanceof Topic)) {
				throw new ValueErrorException('Object of type Topic expected');
			}
			let topicName = topic.getName();
			if (this.existsTopic(topicName) && this.getTopic(topicName) !== topic) {
				throw new DuplicateKeyException(`Topic with name '${topicName}' already exists`);
			}
			let t = this.getTopic(topicName);
			this.#topics[topicName] = topic;
			if (t === undefined) {
				topic.addToManager(this);
			}
		};
		
		/**
		 *  add a topic
		 *  @param topicName
		 */
		existsTopic(topicName) {
			return this.#topics.hasOwnProperty(topicName);
		};
		
		/**
		 *  get topic
		 *  @param topicName
		 */
		getTopic(topicName) {
			if (!this.existsTopic(topicName)) {
				return;
			}
			return this.#topics[topicName];
		};
		
		/**
		 *  get all topics
		 */
		getAllTopics() {
			let t = [];
			Object.values(this.#topics).forEach(topic => t.push(topic));
			return t;
		};
		
		/**
		 *  remove a topic
		 */
		removeTopic(topicName) {
			if (this.existsTopic(topicName)) {
				let t = this.getTopic(topicName);
				delete this.#topics[topicName];
				if (t !== undefined) {
					t.removeFromManager(this);
				}
			}
		};
		
		getObject() {
			let topics = [];
			Object.values(this.#topics).forEach((t) => {
				topics.push(t.getObject());
			});;
			
			return {
				id: this.#id,
				topicCount: topics.length,
				topics: topics
			};
		};
		
		#hasPresubscribers(topicName) {
			return this.#preSubscribers.hasOwnProperty(topicName);
		};
		
		isPresubscribed(topicName, subscriberName) {
			return (this.#hasPresubscribers(topicName) && this.#preSubscribers[topicName].hasOwnProperty(subscriberName));
		};
		
		#addPresubscriber(topicName, subscriberName, callback) {
			if (typeof topicName !== 'string' || topicName.trim().length === 0) {
				throw new ValueErrorException('topicName is invalid');
			} 
			if (typeof subscriberName !== 'string' || subscriberName.trim().length === 0) {
				throw new ValueErrorException('subscriberName is invalid');
			}
			if (typeof callback !== 'function') {
				throw new ValueErrorException('callback is not a function');
			}
			if (!this.#hasPresubscribers(topicName)) {
				this.#preSubscribers[topicName] = {};
				this.#preSubInterval = setInterval(() => { 
					Object.keys(this.#preSubscribers).forEach((t) => {
						Object.keys(this.#preSubscribers[t]).forEach((s) => {
							this.#tryToSubscribePreSubscriber(t, s);
						});
					})
				}, 1000);
			}
			if (this.isPresubscribed(topicName, subscriberName)) {
				throw new DuplicateKeyException(`Subscriber with name '${subscriberName}' already exists`);
			}
			this.#preSubscribers[topicName][subscriberName] = callback;
		};
		
		#removePresubscriber(topicName, subscriberName) {
			if (typeof topicName !== 'string' || topicName.trim().length === 0) {
				throw new ValueErrorException('topicName is invalid');
			} 
			if (typeof subscriberName !== 'string' || subscriberName.trim().length === 0) {
				throw new ValueErrorException('subscriberName is invalid');
			}
			if (this.isPresubscribed(topicName, subscriberName)) {
				delete this.#preSubscribers[topicName][subscriberName];
				if (Object.keys(this.#preSubscribers[topicName]).length === 0) {
					delete this.#preSubscribers[topicName];
				}
				if (Object.keys(this.#preSubscribers).length === 0) {
					clearInterval(this.#preSubInterval);
				}
			}
		};
		
		#tryToSubscribePreSubscriber(topicName, subscriberName) {
			if (this.existsTopic(topicName)) {
				this.getTopic(topicName).subscribe(subscriberName, this.#preSubscribers[topicName][subscriberName]);
				this.#removePresubscriber(topicName, subscriberName);
			}				
		};
		
		subscribeToTopic(topicName, subscriberName, callback) {
			if (this.existsTopic(topicName)) {
				this.getTopic(topicName).subscribe(subscriberName, callback);
			} else {
				this.#addPresubscriber(topicName, subscriberName, callback);
			}
		};
		
		unsubscribeFromTopic(topicName, subscriberName) {
			if (this.existsTopic(topicName)) {
				this.getTopic(topicName).unsubscribe(subscriberName);
			} else {
				this.#removePresubscriber(topicName, subscriberName);
			}
		};
	}
	
	return {
		Exception: Exception,
		ValueErrorException: ValueErrorException,
		DuplicateKeyException: DuplicateKeyException,
		NotFoundException: NotFoundException,
		IllegalOperationException: IllegalOperationException,
		Payload: Payload,
		Event: Event,
		Topic: Topic,
		MessageBroker: MessageBroker
	};
})();