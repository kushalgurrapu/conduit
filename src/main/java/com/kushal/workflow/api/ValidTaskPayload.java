package com.kushal.workflow.api;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** JSON null and NUL bytes are not a legal task payload. */
@Documented
@Constraint(validatedBy = TaskPayloadValidator.class)
@Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.RECORD_COMPONENT})
@Retention(RetentionPolicy.RUNTIME)
public @interface ValidTaskPayload {

    String message() default "payload must not be JSON null or contain a NUL character";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
