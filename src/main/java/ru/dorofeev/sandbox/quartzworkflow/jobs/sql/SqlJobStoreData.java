package ru.dorofeev.sandbox.quartzworkflow.jobs.sql;

import org.springframework.jdbc.core.RowMapper;

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
	}

	private String id;
	private String parentId;
	private String queueName;
	private String executionType;
	private String result;
	private String exception;
	private String jobKey;
	private String args;

	private SqlJobStoreData() {
	}

	SqlJobStoreData(String id, String parentId, String queueName, String executionType, String result, String exception, String jobKey, String args) {
		this.id = id;
		this.parentId = parentId;
		this.queueName = queueName;
		this.executionType = executionType;
		this.result = result;
		this.exception = exception;
		this.jobKey = jobKey;
		this.args = args;
	}

	String getId() {
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

			return data;
		};
	}
}
