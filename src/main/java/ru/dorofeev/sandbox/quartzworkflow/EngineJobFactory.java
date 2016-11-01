package ru.dorofeev.sandbox.quartzworkflow;

import org.quartz.Job;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.spi.JobFactory;
import org.quartz.spi.TriggerFiredBundle;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

class EngineJobFactory implements JobFactory {

	private Map<Class<? extends Job>, Callable<? extends Job>> factories = new HashMap<>();

	void registerFactory(Class<? extends Job> jobType, Callable<? extends Job> jobFactory) {
		factories.put(jobType, jobFactory);
	}

	@Override
	public Job newJob(TriggerFiredBundle bundle, Scheduler scheduler) throws SchedulerException {
		JobDetail jobDetail = bundle.getJobDetail();
		Class<? extends Job> jobClass = jobDetail.getJobClass();
		try {
			return factories.get(jobClass).call();
		} catch (Exception e) {
			throw new SchedulerException(
				"Problem instantiating class '"
					+ jobDetail.getJobClass().getName() + "'", e);
		}
	}
}
