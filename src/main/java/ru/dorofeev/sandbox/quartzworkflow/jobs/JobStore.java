package ru.dorofeev.sandbox.quartzworkflow.jobs;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;

import java.util.Optional;

public interface JobStore {

	Optional<Job> findJob(JobId jobId);

	void recordJobResult(JobId jobId, Job.Result result, Throwable ex);

	Job saveNewJob(JobId parentId, String queueName, QueueingOptions.ExecutionType executionType, JobKey jobKey, Serializable args);

	rx.Observable<Job> traverse(JobId rootId, Job.Result result);
}
