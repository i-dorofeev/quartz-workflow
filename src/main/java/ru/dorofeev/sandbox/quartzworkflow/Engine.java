package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.*;

import java.util.*;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class Engine {

	private final Scheduler scheduler;

	private Map<Class<? extends Event>, Set<Class<? extends EventHandler>>> eventHandlers = new HashMap<>();

	private final JobDetail launchEventHandlersJob;
	private final JobDetail executeEventHandlerJob;

	public Engine(Scheduler scheduler) throws SchedulerException {
		this.scheduler = scheduler;

		EngineJobFactory engineJobFactory = new EngineJobFactory();
		this.scheduler.setJobFactory(engineJobFactory);

		engineJobFactory.registerFactory(ScheduleEventHandlersJob.class, () -> new ScheduleEventHandlersJob(this));
		launchEventHandlersJob = newJob(ScheduleEventHandlersJob.class)
			.storeDurably()
			.build();
		this.scheduler.addJob(launchEventHandlersJob, /* replace */ false);

		engineJobFactory.registerFactory(ExecuteEventHandlerJob.class, () -> new ExecuteEventHandlerJob(this));
		executeEventHandlerJob = newJob(ExecuteEventHandlerJob.class)
			.storeDurably()
			.build();
		this.scheduler.addJob(executeEventHandlerJob, /* replace */ false);
	}

	public void submitEvent(Event event) throws SchedulerException {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(ScheduleEventHandlersJob.PARAM_EVENT, event);

		Trigger trigger = newTrigger()
			.forJob(launchEventHandlersJob.getKey())
			.usingJobData(jobDataMap)
			.startNow()
			.build();

		scheduler.scheduleJob(trigger);
	}

	void submitHandler(Event event, Class<? extends EventHandler> eventHandlerClass) throws SchedulerException {
		JobDataMap jobDataMap = new JobDataMap();
		jobDataMap.put(ExecuteEventHandlerJob.PARAM_EVENT, event);
		jobDataMap.put(ExecuteEventHandlerJob.PARAM_EVENT_HANDLER_CLASS, eventHandlerClass);

		Trigger trigger = newTrigger()
			.forJob(executeEventHandlerJob.getKey())
			.usingJobData(jobDataMap)
			.startNow()
			.build();

		scheduler.scheduleJob(trigger);
	}

	public void registerEventHandler(Class<? extends Event> eventType, Class<? extends EventHandler> eventHandlerType) {
		Set<Class<? extends EventHandler>> handlersForEventType = eventHandlers.get(eventType);
		if (handlersForEventType == null) {
			handlersForEventType = new HashSet<>();
			eventHandlers.put(eventType, handlersForEventType);
		}

		handlersForEventType.add(eventHandlerType);
	}

	Set<Class<? extends EventHandler>> findHandlers(Class<? extends Event> eventType) {
		Set<Class<? extends EventHandler>> handlers = eventHandlers.get(eventType);
		if (handlers == null)
			return Collections.emptySet();
		else
			return Collections.unmodifiableSet(handlers);
	}
}
