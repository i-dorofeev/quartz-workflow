package ru.dorofeev.sandbox.quartzworkflow;

import java.util.List;

public interface EventHandler {

	List<Event> handleEvent(Event event);

	QueueingOption getQueueingOption(Event event);

}
