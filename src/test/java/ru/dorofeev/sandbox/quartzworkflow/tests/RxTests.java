package ru.dorofeev.sandbox.quartzworkflow.tests;

import org.junit.Test;
import rx.subjects.PublishSubject;

public class RxTests {

	@Test
	public void test() {

		PublishSubject<Integer> hotObservable1 = PublishSubject.create();
		PublishSubject<Throwable> errors = PublishSubject.create();

		errors.subscribe(System.out::println);

		hotObservable1
			.map(i ->  {
				if (i % 2 == 0)
					throw new RuntimeException(i.toString());
				return i;
			})
			.retryWhen(e -> e.doOnNext(errors::onNext))
			.subscribe(System.out::println);

		for (int i = 0; i < 10; i++) {
			hotObservable1.onNext(i);
		}

		hotObservable1.onCompleted();
		errors.onCompleted();
	}
}
