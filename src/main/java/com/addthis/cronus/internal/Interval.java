/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.addthis.cronus.internal;

import java.util.BitSet;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractIterator;

/**
 * Immutable representation of a time interval.
 * Each possible index within the interval is either
 * enabled or disabled. Instances are constructed using
 * the {@link Interval.Builder} class.
 */
public class Interval {

    private final BitSet bitSet;

    private final int min;

    private final int max;

    private Interval(int min, int max, BitSet bitSet) {
        this.min = min;
        this.max = max;
        this.bitSet = bitSet;
    }

    public boolean test(int value) {
        Preconditions.checkArgument(min <= value, "Expected min <= value, but %s > %s", min, value);
        Preconditions.checkArgument(max >= value, "Expected max >= value, but %s < %s", max, value);
        value -= min;
        return bitSet.get(value);
    }

    public int getMin() {
        return min;
    }

    public int getMax() {
        return max;
    }

    public boolean isEmpty() {
        return bitSet.isEmpty();
    }

    public boolean isFull() { return bitSet.cardinality() == (max - min + 1); }

    public Iterator<Integer> indexIterator() {
        return indexIterator(min);
    }

    public Iterator<Integer> indexIterator(final int start) {
        Preconditions.checkArgument(min <= start, "Expected min <= start, but %s > %s", min, start);
        Preconditions.checkArgument(max >= start, "Expected max >= start, but %s < %s", max, start);
        return new AbstractIterator<Integer>() {
            int current = start - min;
            protected Integer computeNext() {
                int next = bitSet.nextSetBit(current);
                if (next == -1) {
                    return endOfData();
                } else {
                    current = next + 1;
                    return next + min;
                }
            }
        };
    }

    public static class Builder {

        private final int min;

        private final int max;

        private final BitSet bitSet;

        /**
         * Set all indices in this interval to the value.
         *
         * @return this interval
         */
        public Builder setAll(boolean value) {
            bitSet.set(0, (max - min + 1), value);
            return this;
        }

        /**
         * Set all indices in the range [low, high] to the value
         *
         * @param low    inclusive lower bound
         * @param high   inclusive upper bound
         * @return this interval
         */
        public Builder setRange(int low, int high, boolean value) {
            Preconditions.checkArgument(min <= low, "Expected min <= low, but %s > %s", min, low);
            Preconditions.checkArgument(max >= high, "Expected max >= high, but %s < %s", max, high);
            Preconditions.checkArgument(low <= high, "Expected low <= high, but %s > %s", low, high);
            low -= min;
            high -= min;
            bitSet.set(low, high + 1, value);
            return this;
        }

        /**
         * Set all indices in the range [low, high] to the value stepping through with an increment.
         *
         * @param low    inclusive lower bound
         * @param high   inclusive upper bound
         * @param increment skip interval through the range
         * @return this interval
         */
        public Builder setRange(int low, int high, int increment, boolean value) {
            Preconditions.checkArgument(min <= low, "Expected min <= low, but %s > %s", min, low);
            Preconditions.checkArgument(max >= high, "Expected max >= high, but %s < %s", max, high);
            Preconditions.checkArgument(low <= high, "Expected low <= high, but %s > %s", low, high);
            Preconditions.checkArgument(increment >= 1, "Expected increment >= 1, but %s < 1", increment);
            low -= min;
            high -= min;
            for (int i = low; i <= high; i += increment) {
                bitSet.set(i, value);
            }
            return this;
        }

        /**
         * Set a specific index in the interval to the value.
         *
         * @param index target index
         * @return this interval
         */
        public Builder setIndex(int index, boolean value) {
            Preconditions.checkArgument(min <= index, "Expected min <= index, but %s > %s", min, index);
            Preconditions.checkArgument(max >= index, "Expected max >= index, but %s < %s", max, index);
            index -= min;
            bitSet.set(index, value);
            return this;
        }

        /**
         * Construct an empty interval builder.
         *
         * @param min   lowest allowable value in range
         * @param max  highest allowable value in range
         */
        public Builder(int min, int max) {
            Preconditions.checkArgument(min <= max, "Expected min <= max, but %s > %s", min, max);
            this.min = min;
            this.max = max;
            this.bitSet = new BitSet(max - min + 1);
        }

        public Builder(Interval interval) {
            this.min = interval.min;
            this.max = interval.max;
            this.bitSet = (BitSet) interval.bitSet.clone();
        }

        public Interval build() {
            return new Interval(min, max, bitSet);
        }

    }


    /**
     * Returns the previous index that is set, or null if no index is available.
     */
    public Integer previous(int index, boolean inclusive) {
        Preconditions.checkArgument(min <= index, "Expected min <= index, but %s > %s", min, index);
        Preconditions.checkArgument(max >= index, "Expected max >= index, but %s < %s", max, index);
        if ((index == min) && !inclusive) {
            return null;
        }
        index -= min;
        int result = bitSet.previousSetBit(index - (inclusive ? 0 : 1));
        return (result == -1) ? null : result + min;
    }

    /**
     * Returns the next index that is set, or null if no index is available.
     */
    public Integer next(int index, boolean inclusive) {
        Preconditions.checkArgument(min <= index, "Expected min <= index, but %s > %s", min, index);
        Preconditions.checkArgument(max >= index, "Expected max >= index, but %s < %s", max, index);
        if ((index == max) && !inclusive) {
            return null;
        }
        index -= min;
        int result = bitSet.nextSetBit(index + (inclusive ? 0 : 1));
        return (result == -1) ? null : result + min;
    }
}
