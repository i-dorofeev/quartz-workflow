package ru.dorofeev.sandbox.quartzworkflow.queue.sql;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

class TransactionScope implements AutoCloseable {

	final Session session;
	final Transaction transaction;

	TransactionScope(SessionFactory sessionFactory) {
		this.session = sessionFactory.openSession();
		this.transaction = session.beginTransaction();
	}

	@Override
	public void close() {
		session.close();
	}
}
