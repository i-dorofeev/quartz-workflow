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
import ru.dorofeev.sandbox.quartzworkflow.utils.ExceptionUtils;
import ru.dorofeev.sandbox.quartzworkflow.utils.SqlBuilder;
import ru.dorofeev.sandbox.quartzworkflow.utils.SqlUtils;
import ru.dorofeev.sandbox.quartzworkflow.utils.UUIDGenerator;
import rx.Observable;
import rx.functions.Func0;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static java.lang.Enum.valueOf;
import static java.util.Optional.*;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.Job.Result.CREATED;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.sql.SqlJobStoreData.*;
import static ru.dorofeev.sandbox.quartzworkflow.jobs.sql.SqlJobStoreHierarchy.*;
import static ru.dorofeev.sandbox.quartzworkflow.utils.SqlBuilder.*;
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
			SqlUtils.liquibaseUpdate(dataSource, "sqlJobStore/sqlJobStore.db.changelog.xml");
		} catch (SQLException | LiquibaseException e) {
			throw new JobRepositoryException(e);
		}
	}

	@Override
	public Optional<Job> findJob(JobId jobId) {
		try {
			SqlJobStoreData sqlJobStoreData = jdbcTemplate.queryForObject(

				select("*")
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
		transactionTemplate.execute(status -> {

			jdbcTemplate.update(
					update(TBL_JOB_STORE_DATA)
					.set(sqlEquals(CLMN_RESULT, "?"), sqlEquals(CLMN_EXCEPTION, "?"))
					.where(sqlEquals(CLMN_ID, "?"))
				.sql(), result.toString(), ExceptionUtils.toString(ex), jobId.toString());

			return null;
		});
	}

	@Override
	public Job saveNewJob(JobId parentId, String queueName, ExecutionType executionType, JobKey jobKey, Serializable args) {

		SqlJobStoreData sqlJobStoreData = newJobData(parentId, queueName, executionType, jobKey, args);

		return transactionTemplate.execute(status -> {
			insertJobStoreData.execute(new BeanPropertySqlParameterSource(sqlJobStoreData));

			jdbcTemplate.update(
				insertInto(TBL_JOB_STORE_HIERARCHY)
				.values("?", "?")
				.sql(),
				sqlJobStoreData.getId(), sqlJobStoreData.getParentId());

			jdbcTemplate.update(
				insertInto(TBL_JOB_STORE_HIERARCHY,
					select("?", CLMN_JOB_PARENT_ID)
						.from(TBL_JOB_STORE_HIERARCHY)
						.where(sqlEquals(CLMN_JOB_ID, "?"))
				).sql(),
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
		return (result != null ?
				traverseSubTreeByResult(rootId.toString(), result.toString()) :
				traverseSubTree(rootId.toString()))
			.map(this::fromSqlJobStoreData);
	}

	private Observable<SqlJobStoreData> traverseSubTreeByResult(String rootId, String result) {
		return query(SqlJobStoreData.rowMapper(), () ->

				select("d.*")
					.from(TBL_JOB_STORE_DATA, "d")
					.join(TBL_JOB_STORE_HIERARCHY, "h")
					.on(sqlEquals("d."+CLMN_ID, "h."+CLMN_JOB_ID))
					.where(
						expr(
							sqlEquals("h."+CLMN_JOB_PARENT_ID, "?").or(sqlEquals("d."+CLMN_ID, "?")))
							.and(sqlEquals("d."+CLMN_RESULT, "?"))),

			rootId, rootId, result);
	}

	private Observable<SqlJobStoreData> traverseSubTree(String rootId) {
		return query(SqlJobStoreData.rowMapper(), () ->

				select("d.*")
					.from(TBL_JOB_STORE_DATA, "d")
					.join(TBL_JOB_STORE_HIERARCHY, "h")
					.on(sqlEquals("d."+CLMN_ID, "h."+CLMN_JOB_ID))
					.where(
						sqlEquals("h."+CLMN_JOB_PARENT_ID, "?")
							.or(sqlEquals("d."+CLMN_ID, "?"))),

			rootId, rootId);
	}

	@Override
	public Observable<Job> traverseAll(Job.Result result) {
		return (result != null ?
				traverseAllByResult(result) :
				traverseAll())
			.map(this::fromSqlJobStoreData);
	}

	private Observable<SqlJobStoreData> traverseAllByResult(Job.Result result) {
		return query(SqlJobStoreData.rowMapper(), () ->

			select("*")
				.from(TBL_JOB_STORE_DATA)
				.where(sqlEquals(CLMN_RESULT, "?")), result.toString());
	}

	private Observable<SqlJobStoreData> traverseAll() {
		return query(SqlJobStoreData.rowMapper(), () ->

			select("*")
				.from(TBL_JOB_STORE_DATA));
	}

	private <T> Observable<T> query(RowMapper<T> rowMapper, Func0<SqlBuilder> buildSql, Object... args) {
		SqlBuilder sqlBuilder = buildSql.call();
		String sql = sqlBuilder.sql();
		List<T> results = jdbcTemplate.query(sql, rowMapper, args);
		return from(results);
	}

	@Override
	public Observable<Job> traverseRoots() {
		return query(SqlJobStoreData.rowMapper(), () ->
			select("*")
				.from(TBL_JOB_STORE_DATA)
				.where(isNull(CLMN_PARENT_ID)))

			.map(this::fromSqlJobStoreData);

	}
}
