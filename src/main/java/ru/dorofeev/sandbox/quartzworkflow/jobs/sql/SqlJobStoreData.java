package ru.dorofeev.sandbox.quartzworkflow.jobs.sql;

import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import static ru.dorofeev.sandbox.quartzworkflow.jobs.sql.SqlJobStoreData.Columns.*;

class SqlJobStoreData {

	static final String TBL_JOB_STORE_DATA = "jobstore_data";

	static class Columns {
		static final String CLMN_ID = "id";
		static final String CLMN_PARENT_ID = "parent_id";
		static final String CLMN_QUEUE_NAME = "queue_name";
		static final String CLMN_EXECUTION_TYPE = "execution_type";
		static final String CLMN_JOB_KEY = "job_key";
		static final String CLMN_EXCEPTION = "exception";
		static final String CLMN_RESULT = "result";
		static final String CLMN_ARGS = "args";
		static final String CLMN_CREATED = "created";
		static final String CLMN_EXECUTION_DURATION = "execution_duration";
		static final String CLMN_COMPLETED = "completed";
	}

	private String id;
	private String parentId;
	private String queueName;
	private String executionType;
	private String result;
	private String exception;
	private String jobKey;
	private String args;
	private Timestamp created;
	private Long executionDuration;
	private Timestamp completed;

	private SqlJobStoreData() {
	}

	SqlJobStoreData(String id, String parentId, String queueName, String executionType, String result, String exception, String jobKey, String args, Timestamp created, Long executionDuration, Timestamp completed) {
		this.id = id;
		this.parentId = parentId;
		this.queueName = queueName;
		this.executionType = executionType;
		this.result = result;
		this.exception = exception;
		this.jobKey = jobKey;
		this.args = args;
		this.created = created;
		this.executionDuration = executionDuration;
		this.completed = completed;
	}

	@SuppressWarnings("WeakerAccess") // should be public like all the other getters so that BeanPropertySqlParameterSource could work
	public String getId() {
		return id;
	}

	private void setId(String id) {
		this.id = id;
	}

	public String getParentId() {
		return parentId;
	}

	private void setParentId(String parentId) {
		this.parentId = parentId;
	}

	public String getQueueName() {
		return queueName;
	}

	private void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public String getExecutionType() {
		return executionType;
	}

	private void setExecutionType(String executionType) {
		this.executionType = executionType;
	}

	public String getResult() {
		return result;
	}

	private void setResult(String result) {
		this.result = result;
	}

	public String getException() {
		return exception;
	}

	private void setException(String exception) {
		this.exception = exception;
	}

	public String getJobKey() {
		return jobKey;
	}

	private void setJobKey(String jobKey) {
		this.jobKey = jobKey;
	}

	public String getArgs() {
		return args;
	}

	private void setArgs(String args) {
		this.args = args;
	}

	public Timestamp getCreated() {
		return created;
	}

	public void setCreated(Timestamp created) {
		this.created = created;
	}

	public Long getExecutionDuration() {
		return executionDuration;
	}

	public void setExecutionDuration(Long executionDuration) {
		this.executionDuration = executionDuration;
	}

	public Timestamp getCompleted() {
		return completed;
	}

	public void setCompleted(Timestamp completed) {
		this.completed = completed;
	}

	static RowMapper<SqlJobStoreData> rowMapper() {
		return (rs, rowNum) -> {
			SqlJobStoreData data = new SqlJobStoreData();

			data.setId(rs.getString(CLMN_ID));
			data.setParentId(rs.getString(CLMN_PARENT_ID));
			data.setQueueName(rs.getString(CLMN_QUEUE_NAME));
			data.setExecutionType(rs.getString(CLMN_EXECUTION_TYPE));
			data.setJobKey(rs.getString(CLMN_JOB_KEY));
			data.setException(rs.getString(CLMN_EXCEPTION));
			data.setResult(rs.getString(CLMN_RESULT));
			data.setArgs(rs.getString(CLMN_ARGS));
			data.setCreated(rs.getTimestamp(CLMN_CREATED));
			data.setExecutionDuration(getNullableLong(rs, CLMN_EXECUTION_DURATION));
			data.setCompleted(rs.getTimestamp(CLMN_COMPLETED));

			return data;
		};
	}

	private static Long getNullableLong(ResultSet rs, String column) throws SQLException {
		long value = rs.getLong(column);
		return rs.wasNull() ? null : value;
	}
}
