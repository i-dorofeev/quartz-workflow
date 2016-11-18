package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;

import java.util.List;

public interface EventHandler {

	List<Event> handleEvent(Event event);

	QueueingOptions getQueueingOption(Event event);

}
