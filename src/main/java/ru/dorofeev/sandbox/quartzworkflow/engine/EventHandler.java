package ru.dorofeev.sandbox.quartzworkflow.engine;

import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOption;

import java.util.List;

public interface EventHandler {

	List<Event> handleEvent(Event event);

	QueueingOption getQueueingOption(Event event);

}
