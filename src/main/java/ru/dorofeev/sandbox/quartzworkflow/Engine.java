package ru.dorofeev.sandbox.quartzworkflow;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class Engine {

	private final Scheduler scheduler;
	private final EngineJobFactory jobFactory;
	private final EngineSchedulerListener schedulerListener;

	private Map<Class<? extends Event>, Set<String>> eventHandlers = new HashMap<>();
	private Map<String, EventHandler> eventHandlerInstances = new HashMap<>();

	private final JobKey scheduleEventHandlersJob;
	private final JobKey executeEventHandlerJob;

	public Engine(Class<? extends java.sql.Driver> sqlDriver, String dataSourceUrl) {
		try {
			prepareDatabase(dataSourceUrl);

			StdSchedulerFactory schedulerFactory = new StdSchedulerFactory(getSchedulerProperties(sqlDriver.getName(), dataSourceUrl));
			this.scheduler = schedulerFactory.getScheduler();

			this.jobFactory = new EngineJobFactory();
			this.scheduler.setJobFactory(jobFactory);

			this.schedulerListener = new EngineSchedulerListener();
			this.scheduler.getListenerManager().addSchedulerListener(schedulerListener);

			this.scheduleEventHandlersJob = createJob("scheduleEventHandlers",
				ScheduleEventHandlersJob.class, () -> new ScheduleEventHandlersJob(this));

			this.executeEventHandlerJob = createJob("executeEventHandlers",
				ExecuteEventHandlerJob.class, () -> new ExecuteEventHandlerJob(this));
		} catch (SchedulerException e) {
			throw new EngineException(e);
		}
	}

	public void resetErrors() {
		schedulerListener.resetSchedulerErrors();
	}

	public void assertSuccess() {
		schedulerListener.getSchedulerErrors().forEach(e -> { throw e; });
	}

	private void prepareDatabase(String dataSourceUrl) {
		try {
			JdbcConnection h2Connection = new JdbcConnection(DriverManager.getConnection(dataSourceUrl));
			Database db = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(h2Connection);
			Liquibase liquibase = new Liquibase("engine.db.changelog.xml", new CustomClassLoaderResourceAccessor(Engine.class.getClassLoader()), db);
			liquibase.update(new Contexts());
		} catch (SQLException | LiquibaseException e) {
			throw new EngineException(e);
		}
	}

	private Properties getSchedulerProperties(String sqlDriverName, String dataSourceUrl) {
		Properties p = new Properties();

		p.setProperty("org.quartz.scheduler.instanceName", "DefaultQuartzScheduler");
		p.setProperty("org.quartz.scheduler.rmi.export", "false");
		p.setProperty("org.quartz.scheduler.rmi.proxy", "false");
		p.setProperty("org.quartz.scheduler.wrapJobExecutionInUserTransaction", "false");
		p.setProperty("org.quartz.threadPool.class", "org.quartz.simpl.SimpleThreadPool");
		p.setProperty("org.quartz.threadPool.threadCount", "10");
		p.setProperty("org.quartz.threadPool.threadPriority", "5");
		p.setProperty("org.quartz.threadPool.threadsInheritContextClassLoaderOfInitializingThread", "true");
		p.setProperty("org.quartz.jobStore.misfireThreshold", "6000");

		p.setProperty("org.quartz.jobStore.class", "org.quartz.impl.jdbcjobstore.JobStoreTX");
		p.setProperty("org.quartz.jobStore.dataSource", "ds");
		p.setProperty("org.quartz.dataSource.ds.driver", sqlDriverName);
		p.setProperty("org.quartz.dataSource.ds.URL", dataSourceUrl);


		// p.setProperty("org.quartz.jobStore.class", "org.quartz.simpl.RAMJobStore");

		return p;
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

	private <T extends Job> JobKey createJob(String name, Class<T> jobType, Callable<T> jobTypeFactory) throws SchedulerException {
		jobFactory.registerFactory(jobType, jobTypeFactory);

		JobDetail jobDetail = newJob(jobType)
			.withIdentity(name)
			.storeDurably()
			.build();

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
			.forJob(scheduleEventHandlersJob)
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
