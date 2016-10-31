package ru.dorofeev.sandbox.quartzworkflow;

public interface EventHandler {

	void handleEvent(Engine engine, Event event);
}
