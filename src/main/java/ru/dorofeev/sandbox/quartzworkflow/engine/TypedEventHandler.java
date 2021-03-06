package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;

import java.util.List;

public abstract class TypedEventHandler<T extends Event> implements EventHandler {

	@Override
	public List<Event> handleEvent(Event event) {
		//noinspection unchecked
		T typedEvent = (T) event;
		return handle(typedEvent);
	}

	protected abstract List<Event> handle(T event);

	@Override
	public QueueingOptions getQueueingOption(Event event) {
		return QueueingOptions.DEFAULT;
	}
}
