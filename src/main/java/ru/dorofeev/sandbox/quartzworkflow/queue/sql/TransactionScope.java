package ru.dorofeev.sandbox.quartzworkflow.queue.sql;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

import static ru.dorofeev.sandbox.quartzworkflow.utils.Contracts.shouldNotBeNull;

class TransactionScope implements AutoCloseable {

	final Session session;
	final Transaction transaction;

	TransactionScope(SessionFactory sessionFactory) {

		shouldNotBeNull(sessionFactory, "sessionFactory should be specified");

		this.session = sessionFactory.openSession();
		this.transaction = session.beginTransaction();
	}

	@Override
	public void close() {
		session.close();
	}
}
