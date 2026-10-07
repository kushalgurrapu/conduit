package com.kushal.workflow.api;

import jakarta.validation.ConstraintValidator;
import jakarta.validation.ConstraintValidatorContext;
import tools.jackson.databind.JsonNode;

import java.util.Map;

/**
 * Rejects JSON null and a NUL byte anywhere in an object key or a text
 * value, including nested objects and arrays. A missing value is left to
 * {@code @NotNull}.
 */
public class TaskPayloadValidator implements ConstraintValidator<ValidTaskPayload, JsonNode> {

    @Override
    public boolean isValid(JsonNode value, ConstraintValidatorContext context) {
        if (value == null) {
            return true;
        }
        if (value.isNull()) {
            return violation(context, "payload must not be null");
        }
        if (containsNul(value)) {
            return violation(context, "payload must not contain a NUL character");
        }
        return true;
    }

    private static boolean violation(ConstraintValidatorContext context, String message) {
        context.disableDefaultConstraintViolation();
        context.buildConstraintViolationWithTemplate(message).addConstraintViolation();
        return false;
    }

    private static boolean containsNul(JsonNode node) {
        if (node.isString()) {
            return node.stringValue().indexOf('\0') >= 0;
        }
        if (node.isObject()) {
            for (Map.Entry<String, JsonNode> field : node.properties()) {
                if (field.getKey().indexOf('\0') >= 0 || containsNul(field.getValue())) {
                    return true;
                }
            }
            return false;
        }
        if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
                JsonNode element = node.get(i);
                if (element != null && containsNul(element)) {
                    return true;
                }
            }
        }
        return false;
    }
}
