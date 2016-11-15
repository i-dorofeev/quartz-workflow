package ru.dorofeev.sandbox.quartzworkflow.utils;

import rx.Observable;
import rx.Observer;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.subjects.PublishSubject;

public class ErrorObservable {

	private final PublishSubject<Throwable> errors = PublishSubject.create();

	public Observable<Throwable> asObservable() {
		return errors;
	}

	public Observer<Throwable> asObserver() {
		return errors;
	}

	public void subscribeTo(Observable<Throwable> errors) {
		errors.subscribe(this.errors);
	}

	public <I, O> Observable.Transformer<I, O> mapRetry(Func1<I, O> func) {
		return observable ->
			observable.map(func)
				.retryWhen(errors -> errors.doOnNext(this.errors::onNext)); // publish unhandled exceptions to errors stream
	}

	public <I, F extends I, O> Observable.Transformer<I, O> filterMapRetry(Class<F> filter, Func1<F, O> func) {
		return observable ->
			observable.ofType(filter)
				.compose(mapRetry(func));
	}

	public <I, O> Observable.Transformer<I, O> filterMapRetry(Func1<I, Boolean> filter, Func1<I, O> func) {
		return observable ->
			observable.filter(filter)
				.compose(mapRetry(func));
	}

	public <I> Observable.Transformer<I, I> doOnNextRetry(Action1<? super I> onNext) {
		return iObservable ->
			iObservable.doOnNext(onNext)
				.retryWhen(errors -> errors.doOnNext(this.errors::onNext)); // publish unhandled exceptions to errors stream
	}
}
