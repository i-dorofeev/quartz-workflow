package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.util.*;
import java.util.concurrent.Callable;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class Engine {

	private final Scheduler scheduler;
	private final EngineJobFactory jobFactory;

	private Map<Class<? extends Event>, Set<String>> eventHandlers = new HashMap<>();
	private Map<String, EventHandler> eventHandlerInstances = new HashMap<>();

	private final JobKey launchEventHandlersJob;
	private final JobKey executeEventHandlerJob;

	public Engine() {
		try {
			this.scheduler = StdSchedulerFactory.getDefaultScheduler();
			this.jobFactory = new EngineJobFactory();

			this.scheduler.setJobFactory(jobFactory);

			this.launchEventHandlersJob = createJob(ScheduleEventHandlersJob.class, () -> new ScheduleEventHandlersJob(this));
			this.executeEventHandlerJob = createJob(ExecuteEventHandlerJob.class, () -> new ExecuteEventHandlerJob(this));
		} catch (SchedulerException e) {
			throw new EngineException(e);
		}
	}

	public void start() {
		try {
			scheduler.start();
		} catch (SchedulerException e) {
			throw new EngineException(e);
		}
	}

	public void shutdown() {
		try {
			scheduler.shutdown(true);
		} catch (SchedulerException e) {
			throw new EngineException(e);
		}
	}

	private <T extends Job> JobKey createJob(Class<T> jobType, Callable<T> jobTypeFactory) throws SchedulerException {
		jobFactory.registerFactory(jobType, jobTypeFactory);

		JobDetail jobDetail = newJob(jobType).storeDurably().build();
		scheduler.addJob(jobDetail, /* replace */ true);

		return jobDetail.getKey();
	}

	private void scheduleTrigger(Trigger trigger) {
		try {
			scheduler.scheduleJob(trigger);
		} catch (SchedulerException e) {
			throw new EngineException(e);
		}
	}

	public void submitEvent(Event event) {
		Trigger trigger = newTrigger()
			.forJob(launchEventHandlersJob)
			.usingJobData(ScheduleEventHandlersJob.params(event))
			.startNow()
			.build();

		scheduleTrigger(trigger);
	}

	void submitHandler(Event event, String handlerUri) {
		Trigger trigger = newTrigger()
			.forJob(executeEventHandlerJob)
			.usingJobData(ExecuteEventHandlerJob.params(event, handlerUri))
			.startNow()
			.build();

		scheduleTrigger(trigger);
	}

	Optional<EventHandler> findHandlerByUri(String handlerUri) {
		EventHandler eventHandler = eventHandlerInstances.get(handlerUri);
		return Optional.ofNullable(eventHandler);
	}

	public void registerEventHandlerInstance(String handlerUri, EventHandler eventHandler) {
		EventHandler replacedHandler = eventHandlerInstances.putIfAbsent(handlerUri, eventHandler);
		if (replacedHandler != null)
			throw new EngineException("Event handler " + eventHandler + " is already registered for uri " + handlerUri);
	}

	public void registerEventHandler(Class<? extends Event> eventType, String handlerUri) {
		Set<String> handlersForEventType = eventHandlers.get(eventType);
		if (handlersForEventType == null) {
			handlersForEventType = new HashSet<>();
			eventHandlers.put(eventType, handlersForEventType);
		}

		handlersForEventType.add(handlerUri);
	}

	Set<String> findHandlers(Class<? extends Event> eventType) {
		Set<String> handlers = eventHandlers.get(eventType);
		if (handlers == null)
			return Collections.emptySet();
		else
			return Collections.unmodifiableSet(handlers);
	}
}
