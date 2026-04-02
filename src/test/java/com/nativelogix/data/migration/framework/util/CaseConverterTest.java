package com.nativelogix.rdbms2marklogic.util;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static com.nativelogix.rdbms2marklogic.util.CaseConverter.Case.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CaseConverterTest {

    static Stream<Arguments> conversions() {
        return Stream.of(
            // SNAKE → *
            Arguments.of("my_field_name", SNAKE, CAMEL,   "myFieldName"),
            Arguments.of("my_field_name", SNAKE, PASCAL,  "MyFieldName"),
            Arguments.of("my_field_name", SNAKE, DASH,    "my-field-name"),

            // CAMEL → *
            Arguments.of("myFieldName",   CAMEL, SNAKE,   "my_field_name"),
            Arguments.of("myFieldName",   CAMEL, PASCAL,  "MyFieldName"),
            Arguments.of("myFieldName",   CAMEL, DASH,    "my-field-name"),

            // PASCAL → *
            Arguments.of("MyFieldName",   PASCAL, SNAKE,  "my_field_name"),
            Arguments.of("MyFieldName",   PASCAL, CAMEL,  "myFieldName"),
            Arguments.of("MyFieldName",   PASCAL, DASH,   "my-field-name"),

            // DASH → *
            Arguments.of("my-field-name", DASH, SNAKE,    "my_field_name"),
            Arguments.of("my-field-name", DASH, CAMEL,    "myFieldName"),
            Arguments.of("my-field-name", DASH, PASCAL,   "MyFieldName")
        );
    }

    @ParameterizedTest(name = "\"{0}\" {1} → {2} = \"{3}\"")
    @MethodSource("conversions")
    void convert(String input, CaseConverter.Case from, CaseConverter.Case to, String expected) {
        assertEquals(expected, CaseConverter.convert(input, from, to));
    }
}
