package ru.dorofeev.sandbox.quartzworkflow.utils.entrypoint;

import java.lang.annotation.Retention;

import static java.lang.annotation.RetentionPolicy.SOURCE;

/**
 * Indicates that an annotated item is a public API entry point.
 * One can configure Intellij IDEA not to consider those items as not used.
 */
@Retention(SOURCE)
public @interface API {
}
