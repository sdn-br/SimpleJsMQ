/**
 * SimpleJSMQ
 *
 * Simple Javascript Message Broker System
 *
 * @author Michael Virnstein
 * @version 0.9.0
 * Copyright © 2021 Michael Virnstein
 */
let SimpleJsMQ = (function() {
	
	/**
	 * Class ValueError
	 */
	class ValueError extends Error {
		name = 'ValueError';
		constructor(msg) {
			super(msg);
		};
	}

	/**
	 * Class NotImplememtedError
	 */
	 class NotImplememtedError extends Error {
		name = 'NotImplememtedError';
		constructor(msg) {
			super(msg);
		};
	}

	/**
	 * Class DuplicationError
	 */
	class DuplicationError extends Error {
		name = 'DuplicationError';
		constructor(msg) {
			super(msg);
		};
	}

	/**
	 * Class NotFoundError
	 */
	class NotFoundError extends Error {
		name = 'NotFoundError';
		constructor(msg) {
			super(msg);
		};
	}

	/**
	 * Class IllegalOperationError
	 */
	class IllegalOperationError extends Error {
		name = 'IllegalOperationError';
		constructor(msg) {
			super(msg);
		};
	}

	/**
	 * Class Payload
	 */
	class Payload {
		static _lastId = 0;
				
		/**
		 * Constructor
		 * @param {Event} event 
		 * @param {String} type 
		 * @param {*} data 
		 */
		constructor(event, type, data) {
			if (!(event instanceof Event)) {
				throw new ValueError(`Event expected for event, '${typeof event}' given`);
			}
			if (typeof type != 'string' || type.trim().length === 0) {
				throw new ValueError('type is invalid');
			} 
			if (data === undefined || data === null || typeof data == 'string' && data.trim().length === 0) {
				throw new ValueError('data is invalid');
			}
			
			this._id = ++Payload._lastId;
			this._event = event;
			this._type = type;
			this._data = data;
		};
		
		/**
		 * get Id
		 * @returns Integer
		 */
		getId() {
			return this._id;
		};

		/**
		 * get Event
		 * @returns Event
		 */
		getEvent() {
			return this._event;
		};

		/**
		 * get payload type
		 * @returns String
		 */
		getType() {
			return this._type;
		};
		
		/**
		 * get data
		 * @returns *
		 */
		getData() {
			return this._data; 
		};
		
		/**
		 * get Object copy of Event
		 * @returns Object
		 */
		getObject(cascade = true) {
			return {
				id: this.getId(),
				event: cascade ? this.getEvent().getObject(false) : `[${this.getEvent().constructor.name}]`,
				type: this.getType(),
				data: this.getData()
			}
		};

		/**
		 * get JSON string
		 * @returns String
		 */
		toString() {
			return JSON.stringify(this.getObject());
		};
	}

	/**
	 * Class Event
	 */
	class Event  {
		static _lastId = 0;

		/**
		 * Constructor
		 * @param {EventHandler} eventHandler 
		 * @param {String} eventName 
		 * @param {Payload} payload
		 */
		constructor(eventHandler, eventName, datatype, data) {
			if (!(eventHandler instanceof EventHandler)) {
				throw new ValueError(`EventHandler expected for eventHandler, '${eventHandler && eventHandler.constructor ? eventHandler.constructor.name : typeof eventHandler} given`);
			}
			if (typeof eventName != 'string' || eventName.trim().length === 0) {
				throw new ValueError('eventName is invalid');
			} 
			
			this._id = ++Event._lastId;	
			this._eventHandler = eventHandler;
			this._name = eventName.trim();
			this._payload = this._createPayload(datatype, data);
		};

		/**
		 * 
		 * @param {String} dataType 
		 * @param {*} data 
		 * @returns Payload
		 */
		_createPayload(dataType, data) {
			return new Payload(this, dataType, data);
		};
		
		/**
		 * Get Id
		 * @returns Integer
		 */
		getId() {
			return this._id;
		};
		
		/**
		 * Get EventHandler
		 * @returns EventHandler
		 */
		getEventHandler() {
			return this._eventHandler;
		};
		
		/**
		 * Get name
		 * @returns String
		 */
		getName() {
			return this._name;
		};
		
		/**
		 * get Payload
		 * @returns Payload
		 */
		getPayload() {
			return this._payload;
		};
		
		/**
		 * get object copy
		 * @returns Object
		 */
		getObject(cascade = true) {
			return {
				id: this.getId(),
				eventHandler: cascade ? this.getEventHandler().getObject() : `[${this.getEventHandler().constructor.name}]`,
				name: this.getName(),
				payload: cascade ? this.getPayload().getObject(false) : `[${this.getPayload().constructor.name}]`
			};
		};

		/**
		 * get JSON string
		 * @returns String
		 */
		toString() {
			return JSON.stringify(this.getObject());
		};
		
	}

	/**
	 * Class EventHandler
	 */
	class EventHandler {
		static _lastId = 0;
		/**
		 * constructor
		 * @param {String} name 
		 * @throws ValueError
		 */
		constructor(name, options = {}, broker = undefined) {
			if (typeof name !== 'string' || name.trim().length === 0) {
				throw new ValueError('name is invalid');
			}
			if (broker !== undefined && !(broker instanceof EventBroker)) {
				throw new ValueError(`EventBroker or undefined expected for broker, '${broker && broker.constructor ? broker.constructor.name : typeof broker}' given`);
			}
			
			this._id = ++EventHandler._lastId;
			this._name = name.trim();
			this._subscribers = [];
			this._deliveryFailedCount = 0;
			this._options = {};
			this._eventQueue = [];
			this._queuedCount = 0;
			this._dequeuedCount = 0;
			this.setOptions(options);
			this._broker = undefined;
			if (broker !== undefined) {
				this.manage(broker);
			}
		};

		validateOptions(options) {
			return typeof options !== 'object';
		}
		
		/**
		 * get id
		 * @returns Integer
		 */ 
		getId() {
			return this._id;
		};
		
		/**
		 * get name
		 * @returns String
		 */ 
		getName() {
			return this._name;
		};

		/**
		 * get Object copy of the options
		 * @returns Object
		 */
		getOptions() {
			return Object.assign({}, this._options)
		}
		
		/**
		 * set Options
		 * @param {Object} options 
		 */
		setOptions(options) {
			if (this.validateOptions(options)) {
				throw new ValueError('options are invalid');
			}
			this._options = options;
		}

		/**
		 * creates a new event for this EventHandler
		 * @param {String} eventType
		 * @param {String} dataType
		 * @param {*} data
		 * @returns Event
		 * @throws ValueError
		 */
		createEvent(eventType, dataType, data) {
			return new Event(this, eventType, dataType, data);
		};
		
		/**
		 * set broker
		 * @param {EventBrokerr|undefined} broker
		 * @throws ValueError
		 */
		_setBroker(broker) {
			if (broker !== undefined && !(broker instanceof EventBroker)) {
				throw new ValueError(`Broker or undefined expected for broker, '${typeof broker}' given`);
			}
			this._broker = broker;
		};
		
		/**
		 * get Broker
		 * @returns EventBroker|undefined
		 */
		getBroker() {
			return this._broker;
		};
		
		/**
		 * is managed
		 * @returns Boolean
		 */
		isManaged() {
			return this.getBroker() !== undefined;
		};
		
		/**
		 * add to a broker
		 * @param {EventBroker} broker
		 * @throws IllegalOperationError
		 * @throws ValueError
		 */
		manage(broker) {
			if (this.isManaged() && this.getBroker() !== broker) {
				throw new IllegalOperationError('Already registered to another EventBroker');
			}
			let b = this.getBroker();
			this._setBroker(broker);
			if (b === undefined) {
				broker.addEventHandler(this);
			}
		};
		
		/**
		 * remove from broker
		 */
		unmanage(broker) {
			if (broker !== this.getBroker()) {
				throw new IllegalOperationError('Unmanaged or managed by another broker');
			}
			this._setBroker(undefined);
			broker.removeEventHandler(this.getName());
		};
		
		/**
		 * Get the name of the EventHandler
		 * @returns String
		 */
		getName() {
			return this._name;
		};

		/**
		 * Get number of published messages
		 * @returns Integer
		 */
		getQueuedCount() {
			return this._queuedCount;
		};

		/**
		 * Get number of published messages
		 * @returns Integer
		 */
		getDequeuedCount() {
			return this._dequeuedCount;
		};

		/**
		 * Called after input validation but before any processing
		 * @param {Event} event
		 * @returns Boolean
		 * @protected 
		 */
		_prePublishHook(event) { return true; };

		/**
		 * Called after registering to the queue
		 * @param {Event} event
		 * @protected 
		 */
		_postPublishHook(event) { };

		/**
		 * Queue in an event for publishing
		 * @param {Event} event
		 * @throws ValueError
		 */
		publish(event) {
			if (!(event instanceof Event)) {
				throw new ValueError('Object of type Event expected');
			}
			if (!this._prePublishHook(event)) { return; }
			this._queuedCount++
			this._eventQueue.push(event);
			this._postPublishHook(event);
		};

		/**
		 * Called before value is received from the queue
		 * @param {Function} callback
		 * @returns Boolean
		 * @protected 
		 */
		_preDequeueHook(callback) { return true; };

		/**
		 * Called before value is received from the queue
		 * @param {Function} callback
		 * @returns Boolean
		 * @protected 
		 */
		_postDequeueHook(event, callback) { return true; };

		/**
		 * Called before processing
		 * @param {Event} event
		 * @param {Function} callback
		 * @returns Boolean
		 * @protected 
		 */
		_preCallbackHook(event, callback) { return true; };

		/**
		 * Called after successful processing
		 * @param {Event} event
		 * @param {Function} callback
		 * @throws Error
		 * @protected 
		 */
		_callbackSuccessHook(event, callback) { }; 

		/**
		 * Called at start of exception handling, before unshifting event into queue
		 * @param {Event} event
		 * @param {Function} callback
		 * @param {Exception} failed
		 * @returns Boolean
		 * @protected 
		 */
		_preCallbackFailedHook(event, callback, exception) { return true; };

		/**
		 * Called at end of exception handling, after unshifting event into queue again
		 * @param {Event} event
		 * @param {Function} callback
		 * @param {Exception} exception
		 * @protected 
		 */
		_postCallbackFailedHook(event, callback, exception) { return true; };

		/**
		 * Called at end of receive process
		 * @param {Event} event
		 * @param {Function} callback
		 * @param {Boolean} failed
		 * @protected 
		 */
		 _postReceiveHook(event, callback, failed) { };

		 /**
		 * Take next queued event from the queue and hand it the callback
		 * @throws ValueError
		 */
		receive(callback) {
			if (typeof callback != 'function') {
				throw new ValueError('callback invalid');
			}
			if (!this._preDequeueHook(callback)) { return; }
			let failed = false;
			let event = this._eventQueue.shift();
			if (!this._postDequeueHook(event, callback)) { return ; }
			try {
				if (event !== undefined) {
					if (!this._preCallbackHook(event, callback)) { return; }
					callback(event);
					this._callbackSuccessHook(event, callback);
					++this._dequeuedCount;
				}
			} catch (e) {
				if (!this._preCallbackFailedHook(event, callback, e)) { return; };
				this._eventQueue.unshift(event);
				this._deliveryFailedCount++;
				failed = true;
				if (console && console.log) {
					console.log(e);
				}
				this._postCallbackFailedHook(event, callback, e);
			} finally {
				this._postReceiveHook(event, callback, failed);
			}
		};

		/**
		 * has subscribers
		 * @returns Boolean
		 */ 
		hasSubscribers() {
			return (this._subscribers.length > 0);
		};
		
		/**
		 * get subscriber
		 * @param {String} subscriberName
		 * @returns Array
		 */ 
		getSubscribers(subscriberName) {
			return this._subscribers.filter(subscriber => {
				return subscriberName === subscriber.name;
			});
		};
		
		/**
		 * get all subscribers
		 * @returns Array
		 */ 
		getAllSubscribers() {
			return Array.from(this._subscribers);
		};
		
		/**
		 * Get number of published messages
		 * @returns Integer
		 */
		 getDeliveryFailedCount() {
			return this._deliveryFailedCount;
		};

		/**
		 * called before subscriber is added to the list of subscribers
		 * @param {String} subscriberName 
		 * @param {Function} callback 
		 * @returns Boolean
		 * @throws Error
		 */
		_preSubscribeHook(subscriberName, callback) { return true; };

		/**
		 * called after the subscriber has been added to the list of subscribers
		 * @param {String} subscriberName 
		 * @param {Function} callback 
		 */
		_postSubscribeHook(subscriberName, callback) { };
		
		/**
		 * Adds the provided object to the observers of this observable object
		 * @param {String} subscriberName
		 * @param {Function} callback
		 * @throws ValueError
		 * @throws DuplicationeyError
		 */
		subscribe(subscriberName, callback) {
			if (typeof subscriberName !== 'string' || subscriberName.trim().length === 0) {
				throw new ValueError('subscriberName is invalid');
			}
			if (typeof callback !== 'function') {
				throw new ValueError('callback is not a function');
			}
			subscriberName = subscriberName.trim();
			if (!this._preSubscribeHook(subscriberName, callback)) { return; };
			this._subscribers.push({name: subscriberName, callback: callback});
			this._postSubscribeHook(subscriberName, callback);
		};

		/**
		 * called before subscriber is removed fromthe list of subscribers
		 * @param {String} subscriberName 
		 * @param {Function} callback 
		 * @returns Boolean
		 * @throws Error
		 */
		_preUnsubscribeHook(subscriberName, callback) { return true; };

		/**
		 * called after the subscriber has been removed from the list of subscribers
		 * @param {String} subscriberName 
		 * @param {Function} callback 
		 */
		_postUnsubscribeHook(subscriberName, callback) { };
		
		/**
		 * Removes the provided objects from the observers (if it is among them)
		 * @param {String} subscriberName
		 */
		unsubscribe(subscriberName) {
			if (!this._preUnsubscribeHook(subscriberName)) { return; };
			this._subscribers = this._subscribers.filter((subscriber) => {
				return subscriberName !== subscriber.name;
			});
			this._postUnsubscribeHook(subscriberName);
		};

		/**
		 * returns a object copy of the EventHandler
		 * @returns Object
		 */
		getObject(cascade = true) {
			return {
				id: this.getId(),
				name: this.getName(),
				subscriberCount: this._subscribers.length,
				subscribers: Array.from(this._subscribers),
				queuedCount: this.getQueuedCount(),
				dequeuedCount: this.getDequeuedCount(),
				deliveryFailedCount: this.getDeliveryFailedCount(),
				broker: cascade ? this.getBroker().getObject(false) : `[${this.getBroker().constructor.name}]`
			};
		};

		/**
		 * get JSON string
		 * @returns String
		 */
		toString() {
			return JSON.stringify(this.getObject());
		};
	}

	/**
	 * Class Topic
	 */
	class Topic extends EventHandler {

		/**
		 * 
		 * @param {String} name 
		 * @param {Object} options 
		 * @param {EventBroker} broker 
		 */
		constructor(name, options = {}, broker = undefined) {
			super(name, options, broker);
		}
		
		/**
		 * Publish the messages to all subscribers
		 * @param {Event} event
		 */
		_postPublishHook(event) {
			Promise.resolve().then(() => {
				this.receive(() => {
					this._subscribers.forEach(subscriber => {
						subscriber.callback(event);
					});
				});
			});
		};

		/**
		 * Returns true iff the given object is in the list of observers
		 * @param {String} subscriberName
		 * @returns {boolean}
		 */
		isSubscribed(subscriberName) { 
			if (typeof subscriberName !== 'string' || subscriberName.trim().length === 0) {
				throw new ValueError('subscriberName is invalid');
			}
			return this.getSubscribers(subscriberName).length > 0
		};
		
		/**
		 * Adds the provided object to the observers of this observable object
		 * @param {String} subscriberName
		 * @param {Function} callback
		 * @returns Boolean
		 * @throws DuplicationError
		 */
		 _preSubscribeHook(subscriberName, callback) {
			if (this.isSubscribed(subscriberName)){
				throw new DuplicationError(`Subscriber with name '${subscriberName}' already exists`);
			}
			return true;
		};
	};

	/**
	 * Class Queue
	 */
	 class Queue extends EventHandler {
		
		/**
		 * 
		 * @param {String} name 
		 * @param {Object} options 
		 * @param {EventBroker} broker 
		 */
		constructor(name, options = {}, broker = undefined) {
			super(name, options, broker);
			this._lastSubscriberIndex = -1;
		}

		/**
		 * dispatch an event to a subscriber
		 */
		_dispatchEvent() {
			let idx = this._getNextSubscriberIdx();
			if (idx > -1) {
				this.receive((e) => { 
					this._subscribers[idx].callback(e);
					this._lastSubscriberIndex++;
				});
			}
		}

		/**
		 * Get Next Index
		 * @returns Integer
		 */
		_getNextSubscriberIdx() {
			if (this._subscribers.length === 0) {
				return -1
			}
			let idx = -1;
			if (this._lastSubscriberIndex >= this._subscribers.length - 1) {
				this._lastSubscriberIndex = -1;
				idx = 0;
			} else {
				idx = this._lastSubscriberIndex + 1;
			}
			return idx;
		}
		
		/**
		 * Publish the messages to next subscriber, if present
		 * @param {Event} event
		 */
		_postPublishHook(event) {
			if (this._subscribers.length > 0) {
				Promise.resolve().then(() => {
					this._dispatchEvent();
				});
			}
		};

		/**
		 * Send all queued messages, if 
		 * @param {String} subscriberName 
		 * @param {Function} callback 
		 */
		_postSubscribeHook(subscriberName, callback) {
			if (this._subscribers.length === 1) {
				Promise.resolve().then(() => {
					while (this._eventQueue.length > 0) {
						this._dispatchEvent();
					}
				});
			}
			
		};

		/**
		 * reset last subscriber if necessary
		 * @param {String} subscriberName 
		 * @param {Function} callback 
		 */
		_postUnsubscribeHook(subscriberName, callback) {
			if (this._lastSubscriberIndex >= this._subscribers.length - 1) {
				this._lastSubscriberIndex = -1;
			}
		}
	};

	/**
	 * Event Broker Class
	 */
	class EventBroker {
		static _lastId = 0;
		_id;
		_name;
		_eventHandlers;
		
		/**
		 * Constructor
		 * @param {String} name 
		 */
		constructor(name) {
			if (name !== undefined && name !== null && (typeof name !== 'string' || name.trim().length === 0)) {
				throw new ValueError('name is invalid');
			} 
			this._id = ++EventBroker._lastId;
			this._name = (typeof name === 'string' ? name.trim() : 'EventBroker_'+this._id);
			this._eventHandlers = {};
		};
		
		_capitalizeFirstLetter(str) {
			return str.charAt(0).toUpperCase() + str.slice(1);
		}

		/**
		 * check if given type is subclass of EventHandler
		 * @param {String} type 
		 * @returns Boolean
		 */
		_isValidEventHandlerType(type) {
			type = this._capitalizeFirstLetter(type);
			return type !== 'EventHandler' && SimpleJsMQ.EventHandler.isPrototypeOf(SimpleJsMQ[type]);
		}

		/**
		 * getId	
		 * @returns Integer
		 */
		getId() {
			return this._id;
		};
		
		/**
		 * creates a new managed Event Handler
		 * @param {String} type
		 * @param {String} name
		 * @returns EventHandler
		 */
		createEventHandler(type, name, options = {}, failOnExistence = true) {
			type = this._capitalizeFirstLetter(type);
			if (!this._isValidEventHandlerType(type)) {
				throw new ValueError('type is invalid');
			}
			let eventHandler = this.getEventHandler(name);
			if (eventHandler !== undefined && failOnExistence) {
				throw new DuplicationError(`EventHandler with name '${name}' already exists`);
			} 
			if (eventHandler == undefined) {
				eventHandler = new SimpleJsMQ[type](name, options);
				eventHandler.manage(this);
			}
			return eventHandler;
		};
		
		/**
		 *  add an unmanaged EventHandler to the broker
		 *  @param {Topic} eventHandler
		 */
		addEventHandler(eventHandler) {
			if (!(eventHandler instanceof EventHandler)) {
				throw new ValueError('Object of type EventHandler expected');
			}
			let name = eventHandler.getName();
			if (this.existsEventHandler(name) && this.getEventHandler(name) !== eventHandler) {
				throw new DuplicationError(`EventHandler with name '${name}' already exists`);
			}
			let t = this.getEventHandler(name);
			this._eventHandlers[name] = eventHandler;
			if (t === undefined) {
				eventHandler.manage(this);
			}
		};
		
		/**
		 *  check if EventHandler exists in broker
		 *  @param {String} name
		 *  @returns Boolean
		 */
		existsEventHandler(name) {
			return this._eventHandlers.hasOwnProperty(name.trim());
		};
		
		/**
		 *  get a EventHandler from broker
		 *  @param {String} name
		 *  @returns EventHandler|undefined
		 */
		getEventHandler(name) {
			if (!this.existsEventHandler(name)) {
				return;
			}
			return this._eventHandlers[name.trim()];
		};
		
		/**
		 * get all managed EventHandlers
		 * @returns Array
		 * @throws ValueError
		 */
		getAllEventHandlers(type) {
			if (type != undefined && !this._isValidEventHandlerType(type)) {
				throw new ValueError('type is invalid');
			}
			type = this._capitalizeFirstLetter(type);
			let h = [];
			Object.values(this._eventHandlers).forEach(eventHandler => {
				if (type === undefined || eventHandler.constructor.name === type) {
					h.push(eventHandler);
				}
			});
			return h;
		};
		
		/**
		 *  remove a topic
		 * @param {String} topicName
		 */
		removeEventHandler(name) {
			if (this.existsEventHandler(name)) {
				let h = this.getEventHandler(name);
				delete this._eventHandlers[name.trim()];
				if (h !== undefined) {
					h.unmanage(this);
				}
			}
		};
		
		/**
		 * subscribe to event handler
		 * @param {String} eventHandlerType 
		 * @param {String} eventHandlerName 
		 * @param {Object} eventHandlerOptions 
		 * @param {String} subscriberName 
		 * @param {Function} callback 
		 */
		subscribeToEventHandler(eventHandlerType, eventHandlerName, eventHandlerOptions, subscriberName, callback) {
			let eventHandler = this.getEventHandler(eventHandlerName);			
			if (eventHandler === undefined) {
				eventHandler = this.createEventHandler(eventHandlerType, eventHandlerName, eventHandlerOptions, false);
			}
			eventHandler.subscribe(subscriberName, callback);
		};
		
		/**
		 * unsubscribe to a event handler
		 * @param {String} eventHandlerName 
		 * @param {String} subscriberName 
		 */
		unsubscribeFromEventHandler(eventHandlerName, subscriberName) {
			let eventHandler = this.getEventHandler(eventHandlerName);
			if (eventHandler !== undefined) {
				eventHandler.unsubscribe(subscriberName);
			}
		};
		
		/**
		 * get EventBroker as Object copy
		 * @returns Object
		 */
		getObject(casade = true) {
			let eventHandlers = [];
			Object.values(this._eventHandlers).forEach((h) => {
				eventHandlers.push(casade ? h.getObject() : `[${h.constructor.name}]`);
			});;
			
			return {
				id: this._id,
				eventHandlerCount: eventHandlers.length,
				eventHandlers: eventHandlers
			};
		};

		/**
		 * get JSON string
		 * @returns String
		 */
		 toString() {
			return JSON.stringify(this.getObject());
		};
	}

	/**
	 * Global Exports
	 */
	return {
		ValueError: ValueError,
		DuplicationError: DuplicationError,
		NotFoundError: NotFoundError,
		IllegalOperationError: IllegalOperationError,
		Payload: Payload,
		Event: Event,
		EventHandler: EventHandler,
		Topic: Topic,
		Queue: Queue,
		EventBroker: EventBroker
	};
})();