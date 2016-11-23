package ru.dorofeev.sandbox.quartzworkflow.jobs.sql;

import org.springframework.jdbc.core.RowMapper;

public class SqlJobStoreData {

	public static final String TBL_JOB_STORE_DATA = "jobstore_data";

	public static final String CLMN_ID = "id";
	static final String CLMN_PARENT_ID = "parent_id";
	private static final String CLMN_QUEUE_NAME = "queue_name";
	private static final String CLMN_EXECUTION_TYPE = "execution_type";
	private static final String CLMN_JOB_KEY = "job_key";
	static final String CLMN_EXCEPTION = "exception";
	public static final String CLMN_RESULT = "result";
	private static final String CLMN_ARGS = "args";

	private String id;
	private String parentId;
	private String queueName;
	private String executionType;
	private String result;
	private String exception;
	private String jobKey;
	private String args;

	public SqlJobStoreData() {
	}

	public SqlJobStoreData(String id, String parentId, String queueName, String executionType, String result, String exception, String jobKey, String args) {
		this.id = id;
		this.parentId = parentId;
		this.queueName = queueName;
		this.executionType = executionType;
		this.result = result;
		this.exception = exception;
		this.jobKey = jobKey;
		this.args = args;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getParentId() {
		return parentId;
	}

	public void setParentId(String parentId) {
		this.parentId = parentId;
	}

	public String getQueueName() {
		return queueName;
	}

	public void setQueueName(String queueName) {
		this.queueName = queueName;
	}

	public String getExecutionType() {
		return executionType;
	}

	public void setExecutionType(String executionType) {
		this.executionType = executionType;
	}

	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	public String getException() {
		return exception;
	}

	public void setException(String exception) {
		this.exception = exception;
	}

	public String getJobKey() {
		return jobKey;
	}

	public void setJobKey(String jobKey) {
		this.jobKey = jobKey;
	}

	public String getArgs() {
		return args;
	}

	public void setArgs(String args) {
		this.args = args;
	}

	public static RowMapper<SqlJobStoreData> rowMapper() {
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
