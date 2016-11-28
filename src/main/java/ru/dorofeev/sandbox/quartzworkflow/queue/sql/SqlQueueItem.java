package ru.dorofeev.sandbox.quartzworkflow.queue.sql;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueItem;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueItemStatus;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import ru.dorofeev.sandbox.quartzworkflow.utils.entrypoint.Hibernate;

import javax.persistence.*;

import static javax.persistence.EnumType.STRING;

@Entity
@Table(name = "queue")
public class SqlQueueItem implements QueueItem {

	@Id
	@Column(name = "ordinal", nullable = false, unique = true)
	@GeneratedValue
	private Long ordinal;

	@Column(name = "jobId", nullable = false, unique = true)
	private String jobIdStr;

	@Column(name = "executionType", nullable = false)
	@Enumerated(STRING)
	private ExecutionType executionType;

	@Column(name = "status", nullable = false)
	@Enumerated(STRING)
	private QueueItemStatus status;

	@Version
	private int version;

	@Hibernate
	public SqlQueueItem() {
	}

	public SqlQueueItem(String jobIdStr, ExecutionType executionType, QueueItemStatus status) {
		this.jobIdStr = jobIdStr;
		this.executionType = executionType;
		this.status = status;
	}

	@Hibernate
	public String getJobIdStr() {
		return jobIdStr;
	}

	@Hibernate
	public void setJobId(String jobIdStr) {
		this.jobIdStr = jobIdStr;
	}

	@Override
	public JobId getJobId() {
		return new JobId(jobIdStr);
	}

	@Override
	@Hibernate
	public Long getOrdinal() {
		return ordinal;
	}

	@Hibernate
	public void setOrdinal(Long ordinal) {
		this.ordinal = ordinal;
	}

	@Override
	@Hibernate
	public ExecutionType getExecutionType() {
		return executionType;
	}

	@Hibernate
	public void setExecutionType(ExecutionType executionType) {
		this.executionType = executionType;
	}

	@Override
	@Hibernate
	public QueueItemStatus getStatus() {
		return status;
	}

	@Hibernate
	public void setStatus(QueueItemStatus status) {
		this.status = status;
	}

	@Hibernate
	public int getVersion() {
		return version;
	}

	@Hibernate
	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public String toString() {
		return "SqlQueueItem{" +
			jobIdStr +
			"/" + executionType +
			"/" + status +
			'}';
	}
}
