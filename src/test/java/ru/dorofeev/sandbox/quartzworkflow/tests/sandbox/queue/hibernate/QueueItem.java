package ru.dorofeev.sandbox.quartzworkflow.tests.sandbox.queue.hibernate;

import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;

import javax.persistence.*;

import static javax.persistence.EnumType.STRING;

@Entity
@Table(name = "queue")
public class QueueItem {

	@Id
	@Column(name = "ordinal", nullable = false, unique = true)
	@GeneratedValue
	private int ordinal;

	@Column(name = "id", nullable = false, unique = true)
	private String id;

	@Column(name = "executionType", nullable = false)
	@Enumerated(STRING)
	private ExecutionType executionType;

	@Column(name = "status", nullable = false)
	@Enumerated(STRING)
	private QueueItemStatus status;

	@Version
	private int version;

	public QueueItem() {

	}

	public QueueItem(String id, ExecutionType executionType, QueueItemStatus status) {
		this.id = id;
		this.executionType = executionType;
		this.status = status;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public int getOrdinal() {
		return ordinal;
	}

	public void setOrdinal(int ordinal) {
		this.ordinal = ordinal;
	}

	public ExecutionType getExecutionType() {
		return executionType;
	}

	public void setExecutionType(ExecutionType executionType) {
		this.executionType = executionType;
	}

	public QueueItemStatus getStatus() {
		return status;
	}

	public void setStatus(QueueItemStatus status) {
		this.status = status;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public String toString() {
		return "QueueItem{" +
			id  +
			"/" + executionType +
			"/" + status +
			'}';
	}
}
