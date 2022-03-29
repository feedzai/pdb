/*
 * Copyright 2022 Feedzai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.feedzai.commons.sql.abstraction.dml;

import com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder;
import com.google.common.collect.ImmutableList;
import org.assertj.core.api.AbstractObjectAssert;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.L;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.LofK;
import static com.feedzai.commons.sql.abstraction.dml.dialect.SqlBuilder.k;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for DML classes, without needing a DB engine to test them (mostly the expressions created with {@link SqlBuilder}).
 *
 * @author José Fidalgo (jose.fidalgo@feedzai.com)
 */
public class DmlTest {

    /**
     * Tests that {@link K constant expressions} have a hashcode/equals implementation, that allows them to be compared
     * with each other for equality and to be unique in sets.
     */
    @Test
    public void kHashTest() {
        final Set<K> kSet = new HashSet<>();
        kSet.add(k("a"));
        kSet.add(k("b"));
        kSet.add(k(1));
        kSet.add(k(2));
        kSet.add(k("a"));
        kSet.add(k(1));

        assertThat(kSet)
                .containsExactlyInAnyOrder(k("a"), k("b"), k(1), k(2));
    }

    /**
     * Tests that the expression {@link SqlBuilder#LofK} is a shortcut for and works as a {@link SqlBuilder#L list expression}
     * of {@link K constants}.
     */
    @Test
    public void LofKObjectsTest() {
        final Expression expected = L(k(1), k(2), k("a"));

        assertEqualL(expected, LofK(1, 2, "a"));

        assertEqualL(expected, LofK(ImmutableList.of(1, 2, "a")));
    }

    /**
     * Tests that the expression {@link SqlBuilder#LofK} is a shortcut for and works as a {@link SqlBuilder#L list expression}
     * of {@link K constants}.
     * <p>
     * This test is similar to {@link #LofKObjectsTest()}, but using a collection of elements of the same type (strings
     * in this case). This ensures that {@link SqlBuilder#LofK(Collection)} overload is called when the parameter
     * supplied is a list of strings. If it accepted a collection of {@link Object} instead of {@code ?}, then the
     * overload {@link SqlBuilder#LofK(Object...)} would have been called instead.
     */
    @Test
    public void LofKStringsTest() {
        final Expression expected = L(k("a"), k("b"), k("a"));

        assertEqualL(expected, LofK("a", "b", "a"));

        // make sure the collection has type string
        final Collection<String> constantsList = ImmutableList.of("a", "b", "a");
        assertEqualL(expected, LofK(constantsList));
    }

    /**
     * Tests that {@link SqlBuilder#LofK} properly handles a {@link java.util.stream.Stream} of {@link K constants}.
     * <p>
     * This test uses a stream with a {@link Stream#distinct()} operation, such that the final result is expected to be
     * equal to a {@link SqlBuilder#L list expression} containing distinct constant expressions (as they are collected
     * into a set).
     */
    @Test
    public void LofKstreamTest() {
        final List<Integer> values = ImmutableList.of(1, 2, 3, 2, 5);

        final Expression expected = L(
                values.stream()
                        .map(SqlBuilder::k)
                        .collect(Collectors.toSet())
        );

        assertEqualL(expected, LofK(values.stream().distinct()));
    }

    /**
     * Helper method to asserts that two {@link SqlBuilder#L list expression} are equal.
     * <p>
     * If they are not, an {@link AssertionError} is thrown with an appropriate message. Currently, these expressions
     * are expected to be {@link RepeatDelimiter} with separator {@link RepeatDelimiter#COMMA}; if they are not,
     * an error is thrown. These expressions are considered equal if their delimiter and expressions are equal.
     *
     * @param expected The expected list expression.
     * @param actual   The actual list expression.
     */
    private void assertEqualL(final Expression expected, final Expression actual) {
        assertThat(ImmutableList.of(expected, actual))
                .as("All parameters must be L expressions (class RepeatDelimiter).")
                .allMatch(RepeatDelimiter.class::isInstance);

        final RepeatDelimiter expectedL = (RepeatDelimiter) expected;
        final RepeatDelimiter actualL = (RepeatDelimiter) actual;
        final AbstractObjectAssert<?, RepeatDelimiter> lExprAssertion = assertThat(actualL);

        lExprAssertion.extracting(RepeatDelimiter::getDelimiter)
                .as("Delimiter must match")
                .isEqualTo(RepeatDelimiter.COMMA)
                .as("Delimiter must match")
                .isEqualTo(expectedL.getDelimiter());

        lExprAssertion.extracting(RepeatDelimiter::getExpressions)
                .as("Delimiter must match")
                .isEqualTo(expectedL.getExpressions());
    }
}
