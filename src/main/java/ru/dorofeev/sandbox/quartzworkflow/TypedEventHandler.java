package ru.dorofeev.sandbox.quartzworkflow;

public abstract class TypedEventHandler<T extends Event> implements EventHandler {

	@Override
	public void handleEvent(Engine engine, Event event) {
		//noinspection unchecked
		T typedEvent = (T) event;
		handle(engine, typedEvent);
	}

	protected abstract void handle(Engine engine, T event);
}
