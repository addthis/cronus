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

import java.util.Iterator;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class IntervalTest {

    @Test
    public void indexIterator() {
        Interval interval = new Interval.Builder(1, 10).build();
        Iterator<Integer> iterator = interval.indexIterator();
        assertFalse(iterator.hasNext());
        interval = new Interval.Builder(1, 3).setAll(true).build();
        iterator = interval.indexIterator();
        assertEquals(Integer.valueOf(1), iterator.next());
        assertEquals(Integer.valueOf(2), iterator.next());
        assertEquals(Integer.valueOf(3), iterator.next());
        assertFalse(iterator.hasNext());
        interval = new Interval.Builder(0, 3).setRange(0, 3, 2, true).build();
        iterator = interval.indexIterator();
        assertEquals(Integer.valueOf(0), iterator.next());
        assertEquals(Integer.valueOf(2), iterator.next());
        assertFalse(iterator.hasNext());
        interval = new Interval.Builder(0, 3).setRange(0, 3, 2, true).build();
        iterator = interval.indexIterator(1);
        assertEquals(Integer.valueOf(2), iterator.next());
        assertFalse(iterator.hasNext());
        interval = new Interval.Builder(0, 3).setRange(0, 3, 2, true).build();
        iterator = interval.indexIterator(3);
        assertFalse(iterator.hasNext());
    }
}
