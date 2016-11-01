package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.SchedulerException;
import org.quartz.listeners.SchedulerListenerSupport;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

class EngineSchedulerListener extends SchedulerListenerSupport {

	private final List<EngineException> schedulerErrors = new CopyOnWriteArrayList<>();

	@Override
	public void schedulerError(String msg, SchedulerException cause) {
		schedulerErrors.add(new EngineException(msg, cause));
	}

	List<EngineException> getSchedulerErrors() {
		return schedulerErrors.stream().collect(Collectors.toList());
	}

	void resetSchedulerErrors() {
		schedulerErrors.clear();
	}
}
