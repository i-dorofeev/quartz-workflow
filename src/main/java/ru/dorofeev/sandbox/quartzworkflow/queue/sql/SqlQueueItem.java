package ru.dorofeev.sandbox.quartzworkflow.queue.sql;

import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.NodeId;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueItem;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueItemStatus;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import ru.dorofeev.sandbox.quartzworkflow.utils.entrypoint.Hibernate;

import javax.persistence.*;

import static javax.persistence.EnumType.STRING;
import static ru.dorofeev.sandbox.quartzworkflow.NodeId.ANY_NODE;
import static ru.dorofeev.sandbox.quartzworkflow.queue.sql.SqlQueueItem.UK_QUEUE_JOBID_CONSTRAINT;
import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.shouldNotBeNull;

@Entity
@Table(name = "queue",
       uniqueConstraints = @UniqueConstraint(columnNames = "jobId", name = UK_QUEUE_JOBID_CONSTRAINT))
public class SqlQueueItem implements QueueItem {

	public static final String UK_QUEUE_JOBID_CONSTRAINT = "UK_QUEUE_JOBID";

	@Id
	@Column(name = "ordinal", nullable = false)
	@GeneratedValue
	private Long ordinal;

	@Column(name = "jobId", nullable = false)
	private String jobIdStr;

	@Column(name = "queueName", nullable = false)
	private String queueName;

	@Column(name = "executionType", nullable = false)
	@Enumerated(STRING)
	private ExecutionType executionType;

	@Column(name = "status", nullable = false)
	@Enumerated(STRING)
	private QueueItemStatus status;

	@Column(name = "nodeId")
	private String nodeId;

	@Version
	private int version;

	@Hibernate
	public SqlQueueItem() {
	}

	public SqlQueueItem(String jobIdStr, String queueName, ExecutionType executionType, QueueItemStatus status, String nodeId) {
		this.jobIdStr = jobIdStr;
		this.queueName = queueName;
		this.executionType = executionType;
		this.status = status;
		this.nodeId = nodeId;
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
	public String getQueueName() {
		return queueName;
	}

	@Hibernate
	public void setQueueName(String queueName) {
		this.queueName = queueName;
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
	public String getNodeId() {
		return nodeId;
	}

	@Hibernate
	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
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
			"ordinal=" + ordinal +
			", jobIdStr='" + jobIdStr + '\'' +
			", queueName='" + queueName + '\'' +
			", executionType=" + executionType +
			", status=" + status +
			", nodeId='" + nodeId + '\'' +
			", version=" + version +
			'}';
	}

	public static String fromNodeId(NodeId nodeId) {

		shouldNotBeNull(nodeId, "nodeId should be specified");

		return ANY_NODE.equals(nodeId) ? null : nodeId.toString();
	}
}
