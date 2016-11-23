package ru.dorofeev.sandbox.quartzworkflow.jobs.sql;

import liquibase.exception.LiquibaseException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.simple.SimpleJdbcInsert;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import ru.dorofeev.sandbox.quartzworkflow.JobId;
import ru.dorofeev.sandbox.quartzworkflow.JobKey;
import ru.dorofeev.sandbox.quartzworkflow.jobs.Job;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobRepositoryException;
import ru.dorofeev.sandbox.quartzworkflow.jobs.JobStore;
import ru.dorofeev.sandbox.quartzworkflow.queue.QueueingOptions.ExecutionType;
import ru.dorofeev.sandbox.quartzworkflow.serialization.Serializable;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObject;
import ru.dorofeev.sandbox.quartzworkflow.serialization.SerializedObjectFactory;
import ru.dorofeev.sandbox.quartzworkflow.utils.SqlBuilder;
import ru.dorofeev.sandbox.quartzworkflow.utils.SqlUtils;
import ru.dorofeev.sandbox.quartzworkflow.utils.UUIDGenerator;
import rx.Observable;
import rx.functions.Action1;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static java.lang.Enum.valueOf;
import static java.util.Optional.*;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.Job.Result.CREATED;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.sql.SqlJobStoreData.*;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.sql.SqlJobStoreHierarchy.*;
import static ru.dorofeev.sandbox.quartzworkflow.utils.SqlBuilder.sqlEquals;
import static rx.Observable.from;

public class SqlJobStore implements JobStore {

	private final SerializedObjectFactory serializedObjectFactory;
	private final JdbcTemplate jdbcTemplate;
	private final SimpleJdbcInsert insertJobStoreData;
	private final TransactionTemplate transactionTemplate;
	private final DataSource dataSource;
	private final UUIDGenerator uuidGenerator = new UUIDGenerator();

	public SqlJobStore(DataSource dataSource, SerializedObjectFactory serializedObjectFactory) {
		this.dataSource = dataSource;
		this.jdbcTemplate = new JdbcTemplate(dataSource);
		this.insertJobStoreData = new SimpleJdbcInsert(jdbcTemplate).withTableName(TBL_JOB_STORE_DATA);
		this.transactionTemplate = new TransactionTemplate(new DataSourceTransactionManager(dataSource));
		this.serializedObjectFactory = serializedObjectFactory;
	}

	public void initialize() {
		try {
			SqlUtils.liquibaseUpdate(dataSource, "sqlJobStore.db.changelog.xml");
		} catch (SQLException | LiquibaseException e) {
			throw new JobRepositoryException(e);
		}
	}

	@Override
	public Optional<Job> findJob(JobId jobId) {
		try {
			SqlJobStoreData sqlJobStoreData = jdbcTemplate.queryForObject(new SqlBuilder()
				.select("*")
				.from(TBL_JOB_STORE_DATA)
				.where(sqlEquals(CLMN_ID, "?"))
				.sql(),
				SqlJobStoreData.rowMapper(), jobId.toString());

			return of(fromSqlJobStoreData(sqlJobStoreData));
		} catch (EmptyResultDataAccessException e) {
			return empty();
		}
	}

	@Override
	public void recordJobResult(JobId jobId, Job.Result result, Throwable ex) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Job saveNewJob(JobId parentId, String queueName, ExecutionType executionType, JobKey jobKey, Serializable args) {

		SqlJobStoreData sqlJobStoreData = newJobData(parentId, queueName, executionType, jobKey, args);

		return transactionTemplate.execute(status -> {
			insertJobStoreData.execute(new BeanPropertySqlParameterSource(sqlJobStoreData));

			jdbcTemplate.update(new SqlBuilder()
				.insertInto(TBL_JOB_STORE_HIERARCHY)
				.values("?", "?")
				.sql(),
				sqlJobStoreData.getId(), sqlJobStoreData.getParentId());

			jdbcTemplate.update(new SqlBuilder()
				.insertInto(TBL_JOB_STORE_HIERARCHY)
					.select("?", CLMN_JOB_PARENT_ID)
					.from(TBL_JOB_STORE_HIERARCHY)
					.where(sqlEquals(CLMN_JOB_ID, "?"))
				.sql(),
				sqlJobStoreData.getId(), sqlJobStoreData.getParentId());

			return fromSqlJobStoreData(sqlJobStoreData);
		});
	}

	private Job fromSqlJobStoreData(SqlJobStoreData record) {
		JobId id = new JobId(record.getId());
		JobId parentId = record.getParentId() != null ? new JobId(record.getParentId()) : null;
		String queueName = record.getQueueName();
		ExecutionType executionType = ofNullable(record.getExecutionType()).map(et -> valueOf(ExecutionType.class, et)).orElse(null);
		Job.Result result = ofNullable(record.getResult()).map(r -> valueOf(Job.Result.class, r)).orElse(null);
		String exception = record.getException();
		JobKey jobKey = new JobKey(record.getJobKey());
		SerializedObject args = serializedObjectFactory.spawn(record.getArgs());

		return new Job(id, parentId, queueName, executionType, result, exception, jobKey, args);
	}

	private SqlJobStoreData newJobData(JobId parentId, String queueName, ExecutionType executionType, JobKey jobKey, Serializable args) {
		SerializedObject serializedArgs = serializedObjectFactory.spawn();
		args.serializeTo(serializedArgs);

		return new SqlJobStoreData(
			uuidGenerator.newUuid(),
			parentId != null ? parentId.toString() : null,
			queueName,
			executionType.toString(),
			CREATED.toString(),
			null,
			jobKey.toString(),
			serializedArgs.build());
	}

	@Override
	public Observable<Job> traverseSubTree(JobId rootId, Job.Result result) {
		throw new UnsupportedOperationException("Not implemented yet");
	}

	@Override
	public Observable<Job> traverseAll(Job.Result result) {
		if (result != null) {
			return query(sql -> sql
						.select("*")
						.from(TBL_JOB_STORE_DATA)
						.where(sqlEquals(CLMN_RESULT, "?")), SqlJobStoreData.rowMapper(), result.toString())
				.map(this::fromSqlJobStoreData);
		} else {
			return query(sql -> sql
						.select("*")
						.from(TBL_JOB_STORE_DATA)
						.sql(), SqlJobStoreData.rowMapper())
				.map(this::fromSqlJobStoreData);
		}
	}

	private <T> Observable<T> query(Action1<SqlBuilder> buildSql, RowMapper<T> rowMapper, Object... args) {
		SqlBuilder sqlBuilder = new SqlBuilder();
		buildSql.call(sqlBuilder);

		List<T> results = jdbcTemplate.query(sqlBuilder.sql(), rowMapper, args);
		return from(results);
	}

	@Override
	public Observable<Job> traverseRoots() {
		throw new UnsupportedOperationException("Not implemented yet");
	}
}
