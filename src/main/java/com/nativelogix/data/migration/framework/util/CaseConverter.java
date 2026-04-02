package com.nativelogix.rdbms2marklogic.util;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts field/column names between naming conventions.
 */
public class CaseConverter {

    public enum Case {
        SNAKE,   // my_field_name
        CAMEL,   // myFieldName
        PASCAL,  // MyFieldName
        DASH     // my-field-name
    }

    public static String convert(String input, Case from, Case to) {
        if (input == null || input.isEmpty()) return input;
        List<String> words = tokenize(input, from);
        return format(words, to);
    }

    private static List<String> tokenize(String input, Case from) {
        String[] parts;
        switch (from) {
            case SNAKE:
                parts = input.split("_", -1);
                break;
            case DASH:
                parts = input.split("-", -1);
                break;
            case CAMEL:
            case PASCAL:
                // Split on uppercase letters, preserving them as word starts
                parts = input.split("(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])");
                break;
            default:
                throw new IllegalArgumentException("Unknown case: " + from);
        }
        return Arrays.stream(parts)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
    }

    private static String format(List<String> words, Case to) {
        switch (to) {
            case SNAKE:
                return String.join("_", words);
            case DASH:
                return String.join("-", words);
            case CAMEL:
                return words.get(0) + words.stream()
                        .skip(1)
                        .map(CaseConverter::capitalize)
                        .collect(Collectors.joining());
            case PASCAL:
                return words.stream()
                        .map(CaseConverter::capitalize)
                        .collect(Collectors.joining());
            default:
                throw new IllegalArgumentException("Unknown case: " + to);
        }
    }

    private static String capitalize(String word) {
        if (word.isEmpty()) return word;
        return Character.toUpperCase(word.charAt(0)) + word.substring(1);
    }
}
