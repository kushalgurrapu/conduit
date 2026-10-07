package com.kushal.workflow.api;

import com.kushal.workflow.task.TaskNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.net.URI;

/**
 * RFC 9457 bodies for the task API. Other controllers, including
 * actuator, keep their own error handling.
 *
 * <p>Validation failures, unreadable JSON, and a path value that is not
 * the declared type (a task id that is not a UUID) are handled by
 * {@link ResponseEntityExceptionHandler}. Those are {@code 400}. This
 * class adds the cases that handler does not know: a missing task, a
 * cancel the state machine refused, and anything else.
 *
 * <p>The fallback is a generic {@code 500}. The cause is logged and is
 * not copied into the body, so a database error does not become part of
 * the response.
 */
@RestControllerAdvice(basePackageClasses = TaskController.class)
public class ApiExceptionHandler extends ResponseEntityExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler(TaskNotFoundException.class)
    ProblemDetail handleNotFound(TaskNotFoundException ex, WebRequest request) {
        return problem(HttpStatus.NOT_FOUND, ex.getMessage(), request);
    }

    @ExceptionHandler(IllegalTaskTransitionException.class)
    ProblemDetail handleConflict(IllegalTaskTransitionException ex, WebRequest request) {
        return problem(HttpStatus.CONFLICT, ex.getMessage(), request);
    }

    @ExceptionHandler(Exception.class)
    ProblemDetail handleUnexpected(Exception ex, WebRequest request) {
        log.error("Request failed", ex);
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "The request could not be completed", request);
    }

    private static ProblemDetail problem(HttpStatus status, String detail, WebRequest request) {
        ProblemDetail body = ProblemDetail.forStatusAndDetail(status, detail);
        if (request instanceof ServletWebRequest servletRequest) {
            body.setInstance(URI.create(servletRequest.getRequest().getRequestURI()));
        }
        return body;
    }
}
