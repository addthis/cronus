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
package com.addthis.cronus;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import com.addthis.cronus.internal.TimePeriod;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class CronPatternTest {

    @Test
    public void isEmpty() throws Exception {
        CronPattern pattern = new CronPattern();
        assertTrue(pattern.isEmpty());
        pattern = pattern.setIndex(TimePeriod.MINUTE, 0, true);
        assertTrue(pattern.isEmpty());
        pattern = pattern.setIndex(TimePeriod.HOUR, 0, true);
        assertTrue(pattern.isEmpty());
        pattern = pattern.setIndex(TimePeriod.MONTH, 1, true);
        assertTrue(pattern.isEmpty());
        pattern = pattern.setIndex(TimePeriod.DAYOFWEEK, 0, true);
        assertFalse(pattern.isEmpty());

        pattern = new CronPattern();
        assertTrue(pattern.isEmpty());
        pattern = pattern.setIndex(TimePeriod.MINUTE, 0, true);
        assertTrue(pattern.isEmpty());
        pattern = pattern.setIndex(TimePeriod.HOUR, 0, true);
        assertTrue(pattern.isEmpty());
        pattern = pattern.setIndex(TimePeriod.MONTH, 1, true);
        assertTrue(pattern.isEmpty());
        pattern = pattern.setIndex(TimePeriod.DAYOFMONTH, 1, true);
        assertFalse(pattern.isEmpty());

        pattern = CronPattern.build("* * 31 4 *");
        assertTrue(pattern.isEmpty());
        pattern = CronPattern.build("* * 30,31 2,3 *");
        assertFalse(pattern.isEmpty());

    }

    private static ZonedDateTime createDateTime(LocalDateTime localDateTime) {
        return ZonedDateTime.of(localDateTime, ZoneId.of("UTC"));
    }

    private static ZonedDateTime createEasternTime(LocalDateTime localDateTime) {
        return ZonedDateTime.of(localDateTime, ZoneId.of("America/New_York"));
    }

    private static ZonedDateTime createEasternTime(LocalDateTime localDateTime, ZoneOffset offset) {
        return ZonedDateTime.ofInstant(localDateTime, offset, ZoneId.of("America/New_York"));
    }


    @Test
    public void matches() throws Exception {
        ZonedDateTime dateTime = ZonedDateTime.now();
        CronPattern pattern = new CronPattern();
        assertFalse(pattern.matches(dateTime));
        pattern = CronPattern.build("* * * * *");
        assertTrue(pattern.matches(dateTime));
        pattern = CronPattern.build("* * 31 * *");
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 1, 0, 0));
        assertFalse(pattern.matches(dateTime));
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 0, 0));
        assertTrue(pattern.matches(dateTime));
        pattern = CronPattern.build("* * 31 * 0");
        assertTrue(pattern.matches(dateTime));
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 2, 0, 0));
        assertTrue(pattern.matches(dateTime));
        pattern = CronPattern.build("* * 31 1 0");
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 0, 0));
        assertTrue(pattern.matches(dateTime));
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 2, 0, 0));
        assertTrue(pattern.matches(dateTime));
        dateTime = createDateTime(LocalDateTime.of(2000, 3, 31, 0, 0));
        assertFalse(pattern.matches(dateTime));
    }

    @Test
    public void next() throws Exception {
        ZonedDateTime dateTime = ZonedDateTime.now();
        CronPattern pattern = new CronPattern();
        assertNull(pattern.next(dateTime, true));
        pattern = CronPattern.build("* * * * *");
        assertEquals(dateTime, pattern.next(dateTime, true));
        pattern = CronPattern.build("* 1 * * *");
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 0, 0));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 1, 0)), pattern.next(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 1, 0)), pattern.next(dateTime, false));
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 1, 59));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 1, 59)), pattern.next(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 2, 1, 1, 0)), pattern.next(dateTime, false));
        pattern = CronPattern.build("15,30 3 * * *");
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 2, 15));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 3, 15)), pattern.next(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 3, 15)), pattern.next(dateTime, false));
        pattern = CronPattern.build("59 23 * * *");
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 23, 59));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 23, 59)), pattern.next(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 2, 1, 23, 59)), pattern.next(dateTime, false));
        pattern = CronPattern.build("50 23 * * *");
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 23, 50));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 23, 50)), pattern.next(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 2, 1, 23, 50)), pattern.next(dateTime, false));
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 23, 51));
        assertEquals(createDateTime(LocalDateTime.of(2000, 2, 1, 23, 50)), pattern.next(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 2, 1, 23, 50)), pattern.next(dateTime, false));
    }

    @Test
    public void nextDaylightSavings() throws Exception {
        ZonedDateTime dateTime;
        CronPattern pattern;
        // fall back 2:00 am to 1:00 am Sunday, November 1, 2015
        pattern = CronPattern.build("30 1 * * *");
        /**
         *  If the input is the 1st instance of 1:20 AM
         *  then the output is 1:30 AM on the same day.
         */
        dateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 20), ZoneOffset.ofHours(-4));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-4)),
                                       pattern.next(dateTime, true));
        /**
         *  If the input is the 2st instance of 1:20 AM
         *  then the output is 1:30 AM on the next day.
         */
        dateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 20), ZoneOffset.ofHours(-5));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 2, 1, 30), ZoneOffset.ofHours(-5)),
                     pattern.next(dateTime, true));
        /**
         *  If the input is the 1st instance of 1:30 AM (inclusive)
         *  then the output is the current time.
         */
        dateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-4));
        assertEquals(dateTime, pattern.next(dateTime, true));
        /**
         *  If the input is the 1st instance of 1:30 AM (exclusive)
         *  then the output is the next day.
         */
        dateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-4));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 2, 1, 30)),
                     pattern.next(dateTime, false));
        /**
         *  If the input is the 2st instance of 1:30 AM (inclusive)
         *  then the output is the next day.
         */
        dateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-5));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 2, 1, 30)),
                     pattern.next(dateTime, true));
        /**
         *  If the input is the 2st instance of 1:30 AM (exclusive)
         *  then the output is the next day.
         */
        dateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-5));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 2, 1, 30)),
                     pattern.next(dateTime, false));
        // spring forward 1:59 am to 3:00 am Sunday, March 8, 2015
        dateTime = createEasternTime(LocalDateTime.of(2015, 3, 8, 0, 0));
        pattern = CronPattern.build("30 2 * * *");
        assertEquals(createEasternTime(LocalDateTime.of(2015, 3, 8, 3, 0)),
                     pattern.next(dateTime, true));
        dateTime = createEasternTime(LocalDateTime.of(2015, 3, 8, 1, 59));
        pattern = CronPattern.build("30 2 * * *");
        assertEquals(createEasternTime(LocalDateTime.of(2015, 3, 8, 3, 0)),
                     pattern.next(dateTime, true));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 3, 8, 3, 0)),
                     pattern.next(dateTime, false));
    }

    @Test
    public void previous() throws Exception {
        ZonedDateTime dateTime = ZonedDateTime.now();
        CronPattern pattern = new CronPattern();
        assertNull(pattern.previous(dateTime, true));
        pattern = CronPattern.build("* * * * *");
        assertEquals(dateTime, pattern.previous(dateTime, true));
        pattern = CronPattern.build("* 1 * * *");
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 2, 0));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 1, 59)), pattern.previous(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 1, 59)), pattern.previous(dateTime, false));
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 1, 0));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 1, 0)), pattern.previous(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 30, 1, 59)), pattern.previous(dateTime, false));
        pattern = CronPattern.build("15,30 3 * * *");
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 2, 15));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 30, 3, 30)), pattern.previous(dateTime, false));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 30, 3, 30)), pattern.previous(dateTime, true));
        pattern = CronPattern.build("0 0 * * *");
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 0, 0));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 0, 0)), pattern.previous(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 30, 0, 0)), pattern.previous(dateTime, false));
        pattern = CronPattern.build("10 0 * * *");
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 0, 10));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 31, 0, 10)), pattern.previous(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 30, 0, 10)), pattern.previous(dateTime, false));
        dateTime = createDateTime(LocalDateTime.of(2000, 1, 31, 0, 9));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 30, 0, 10)), pattern.previous(dateTime, true));
        assertEquals(createDateTime(LocalDateTime.of(2000, 1, 30, 0, 10)), pattern.previous(dateTime, false));
    }

    @Test
    public void previousDaylightSavings() throws Exception {
        ZonedDateTime inputDateTime;
        CronPattern pattern;
        // fall back 1:59 am to 1:00 am Sunday, November 1, 2015
        inputDateTime = createEasternTime(LocalDateTime.of(2015, 11, 2, 0, 0));
        pattern = CronPattern.build("30 1 * * *");
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30)),
                     pattern.previous(inputDateTime, true));
        /**
         *  If the input is the 2nd instance of 1:40 AM
         *  then the output is the 1st instance of 1:30 AM.
         */
        inputDateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 40), ZoneOffset.ofHours(-5));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30)),
                     pattern.previous(inputDateTime, true));
        /**
         *  If the input is the 2nd instance of 1:20 AM
         *  then the output is the 1st instance of 1:30 AM.
         */
        inputDateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 20), ZoneOffset.ofHours(-5));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30)),
                     pattern.previous(inputDateTime, true));
        /**
         *  If the input is the 1st instance of 1:30 AM (inclusive)
         *  then the output is 1st instance of 1:30 AM.
         */
        inputDateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-4));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-4)),
                     pattern.previous(inputDateTime, true));
        /**
         *  If the input is the 1st instance of 1:30 AM (exclusive)
         *  then the output is previous day.
         */
        inputDateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-4));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 10, 31, 1, 30), ZoneOffset.ofHours(-4)),
                     pattern.previous(inputDateTime, false));
        /**
         *  If the input is the 2nd instance of 1:30 AM (inclusive)
         *  then the output is the 1st instance of 1:30 AM.
         */
        inputDateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-5));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-4)),
                     pattern.previous(inputDateTime, true));
        /**
         *  If the input is the 2nd instance of 1:30 AM (exclusive)
         *  then the output is the 1st instance of 1:30 AM.
         */
        inputDateTime = createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-5));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 11, 1, 1, 30), ZoneOffset.ofHours(-4)),
                     pattern.previous(inputDateTime, false));
        // spring forward 1:59 am to 3:00 am Sunday, March 8, 2015
        inputDateTime = createEasternTime(LocalDateTime.of(2015, 3, 8, 4, 0));
        pattern = CronPattern.build("30 2 * * *");
        assertEquals(createEasternTime(LocalDateTime.of(2015, 3, 8, 3, 0)),
                     pattern.previous(inputDateTime, true));
        // If the input is 3:01 am then the output is the adjusted 2:30 am
        // which is 3:00 am
        inputDateTime = createEasternTime(LocalDateTime.of(2015, 3, 8, 3, 1));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 3, 8, 3, 0)),
                     pattern.previous(inputDateTime, true));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 3, 8, 3, 0)),
                     pattern.previous(inputDateTime, false));
        // If the input is 3:00 am (inclusive) then the output is 3:00 am
        inputDateTime = createEasternTime(LocalDateTime.of(2015, 3, 8, 3, 0));
        assertEquals(createEasternTime(LocalDateTime.of(2015, 3, 8, 3, 0)),
                     pattern.previous(inputDateTime, true));
        // If the input is 3:00 am (exclusive) then the output is 2:30 am
        // on the previous day
        assertEquals(createEasternTime(LocalDateTime.of(2015, 3, 7, 2, 30)),
                     pattern.previous(inputDateTime, false));
    }

}
