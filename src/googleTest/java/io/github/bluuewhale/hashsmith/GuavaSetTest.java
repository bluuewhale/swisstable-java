package io.github.bluuewhale.hashsmith;

import com.google.common.collect.testing.*;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import com.google.common.collect.testing.features.SetFeature;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jspecify.annotations.NullMarked;

import java.util.*;
import java.util.function.Supplier;

@NullMarked
public final class GuavaSetTest extends TestCase {

    public static Test suite() {
        var suite = new TestSuite();
        suite.addTest(setTest("SwissSet", generator(SwissSet::new)));
        return suite;
    }

    private static Test setTest(String name, TestSetGenerator<?> generator) {
        // FIXME: Enable CollectionFeature.FAILS_FAST_ON_CONCURRENT_MODIFICATION?
        return SetTestSuiteBuilder
            .using(generator)
            .named(name)
            .withFeatures(
                CollectionSize.ANY,
                SetFeature.GENERAL_PURPOSE,
                CollectionFeature.ALLOWS_NULL_VALUES,
                CollectionFeature.SUPPORTS_ITERATOR_REMOVE)
            .createTestSuite();
    }

    private static TestStringSetGenerator generator(Supplier<Set<String>> supplier) {
        return new TestStringSetGenerator() {
            @Override protected Set<String> create(String[] elements) {
                Set<String> set = supplier.get();
                set.addAll(Arrays.asList(elements));
                return set;
            }
        };
    }
}
