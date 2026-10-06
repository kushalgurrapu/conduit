package com.kushal.workflow.worker;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Indexes every {@link TaskHandler} bean by its type. There is no registry
 * type: the map is the index, and building it is what fails startup.
 */
@Configuration
public class HandlerConfiguration {

    /**
     * Same pattern as {@code tasks_task_type_format_check}. A handler whose
     * type cannot be stored can never be claimed, so it is rejected here.
     */
    static final Pattern TASK_TYPE = Pattern.compile("^[A-Z][A-Z0-9_]{0,63}$");

    @Bean(name = "taskHandlers")
    Map<String, TaskHandler> taskHandlers(List<TaskHandler> handlers) {
        return index(handlers);
    }

    static Map<String, TaskHandler> index(List<TaskHandler> handlers) {
        Map<String, TaskHandler> byType = new LinkedHashMap<>();
        for (TaskHandler handler : handlers) {
            String type = handler.type();
            if (type == null || !TASK_TYPE.matcher(type).matches()) {
                throw new IllegalStateException(
                        "Task handler " + handler.getClass().getName()
                                + " has type '" + type
                                + "', which does not match " + TASK_TYPE.pattern());
            }
            TaskHandler existing = byType.putIfAbsent(type, handler);
            if (existing != null) {
                throw new IllegalStateException(
                        "Duplicate task handler for type " + type
                                + ": " + existing.getClass().getName()
                                + " and " + handler.getClass().getName());
            }
        }
        return Map.copyOf(byType);
    }
}
