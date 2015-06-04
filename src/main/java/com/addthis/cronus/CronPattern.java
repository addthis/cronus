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

import java.util.Iterator;

import java.text.ParseException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.MonthDay;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.zone.ZoneOffsetTransition;
import java.time.zone.ZoneRules;

import com.addthis.cronus.internal.Interval;
import com.addthis.cronus.internal.TimePeriod;

import com.google.common.base.Preconditions;

import static com.addthis.cronus.internal.TimePeriod.MINUTE;
import static com.addthis.cronus.internal.TimePeriod.HOUR;
import static com.addthis.cronus.internal.TimePeriod.DAYOFMONTH;
import static com.addthis.cronus.internal.TimePeriod.MONTH;
import static com.addthis.cronus.internal.TimePeriod.DAYOFWEEK;

/**
 * Immutable cron pattern. Patterns can be created from
 * {@link CronPattern#build(String)} or from one of the
 * copying setter methods.
 */
public class CronPattern {

    final Interval minute;

    final Interval hour;

    final Interval dayOfMonth;

    final Interval month;

    final Interval dayOfWeek;

    final String source;

    final boolean isEmpty;

    public static CronPattern build(String pattern) throws ParseException {
        return CronParser.parse(pattern);
    }

    CronPattern() {
        this.minute = MINUTE.interval();
        this.hour = HOUR.interval();
        this.dayOfMonth = DAYOFMONTH.interval();
        this.month = MONTH.interval();
        this.dayOfWeek = DAYOFWEEK.interval();
        this.isEmpty = true;
        this.source = CronParser.print(this);
    }

    CronPattern(Interval minute, Interval hour, Interval dayOfMonth, Interval month, Interval dayOfWeek, String source) {
        this.minute = MINUTE.validate(minute);
        this.hour = HOUR.validate(hour);
        this.dayOfMonth = DAYOFMONTH.validate(dayOfMonth);
        this.month = MONTH.validate(month);
        this.dayOfWeek = DAYOFWEEK.validate(dayOfWeek);
        this.isEmpty = calculateIsEmpty();
        this.source = (source != null) ? source : CronParser.print(this);
    }

    public boolean isEmpty() {
        return isEmpty;
    }

    public boolean matches(Temporal candidate) {
        if (isEmpty()) {
            return false;
        } else {
            return minuteHourMatches(candidate) && dayMatches(candidate);
        }
    }

    public CronPattern setInterval(TimePeriod period, Interval interval) {
        Preconditions.checkNotNull(period, "period argument must be non-null");
        Preconditions.checkNotNull(interval, "interval argument must be non-null");
        interval = period.validate(interval);
        switch (period) {
            case MINUTE:
                return new CronPattern(interval, hour, dayOfMonth, month, dayOfWeek, null);
            case HOUR:
                return new CronPattern(minute, interval, dayOfMonth, month, dayOfWeek, null);
            case DAYOFMONTH:
                return new CronPattern(minute, hour, interval, month, dayOfWeek, null);
            case MONTH:
                return new CronPattern(minute, hour, dayOfMonth, interval, dayOfWeek, null);
            case DAYOFWEEK:
                return new CronPattern(minute, hour, dayOfMonth, month, interval, null);
            default:
                throw new IllegalStateException("unknown time period " + period);
        }
    }

    public Interval getInterval(TimePeriod period) {
        Preconditions.checkNotNull(period, "period argument must be non-null");
        switch (period) {
            case MINUTE:
                return minute;
            case HOUR:
                return hour;
            case DAYOFMONTH:
                return dayOfMonth;
            case MONTH:
                return month;
            case DAYOFWEEK:
                return dayOfWeek;
            default:
                throw new IllegalStateException("unknown time period " + period);
        }
    }

    /**
     * Return the next date time the pattern will fire.
     *
     * @param input       input date time
     * @param inclusive   if true then test input date time
     * @return next date time the pattern will fire
     */
    public LocalDateTime next(LocalDateTime input, boolean inclusive) {
        Preconditions.checkNotNull(input, "input argument must be non-null");
        return (LocalDateTime) nextTemporal(input, inclusive);
    }

    /**
     * Return the next date time the pattern will fire.
     *
     * @param input       input date time
     * @param inclusive   if true then test input date time
     * @return next date time the pattern will fire
     */
    public ZonedDateTime next(ZonedDateTime input, boolean inclusive) {
        Preconditions.checkNotNull(input, "input argument must be non-null");
        if (handleZoneTransition()) {
            ZonedDateTime adjustedInput = inputDaylightSavingsNext(input);
            if (!input.equals(adjustedInput)) {
                input = adjustedInput;
                inclusive = true;
            }
            LocalDateTime localOutput = next(input.toLocalDateTime(), inclusive);
            return outputAdjustDaylightSavings(localOutput, input.getZone());
        } else {
            return (ZonedDateTime) nextTemporal(input, inclusive);
        }
    }

    /**
     * Return the previous date time the pattern did fire.
     *
     * @param input       input date time
     * @param inclusive   if true then test input date time
     * @return previous date time the pattern did fire
     */
    public LocalDateTime previous(LocalDateTime input, boolean inclusive) {
        Preconditions.checkNotNull(input, "input argument must be non-null");
        return (LocalDateTime) previousTemporal(input, inclusive);
    }

    /**
     * Return the previous date time the pattern will fire.
     *
     * @param input       input date time
     * @param inclusive   if true then test input date time
     * @return previous date time the pattern did fire
     */
    public ZonedDateTime previous(ZonedDateTime input, boolean inclusive) {
        Preconditions.checkNotNull(input, "input argument must be non-null");
        if (handleZoneTransition()) {
            ZonedDateTime adjustedInput = inputDaylightSavingsPrevious(input, inclusive);
            if (!input.equals(adjustedInput)) {
                input = adjustedInput;
                inclusive = true;
            }
            LocalDateTime localOutput = previous(input.toLocalDateTime(), inclusive);
            return outputAdjustDaylightSavings(localOutput, input.getZone());
        } else {
            return (ZonedDateTime) previousTemporal(input, inclusive);
        }
    }

    /**
     * Returns a cron pattern with all indices set to value.
     */
    public CronPattern setAll(TimePeriod period, boolean value) {
        Preconditions.checkNotNull(period, "period argument must be non-null");
        return setInterval(period, new Interval.Builder(getInterval(period)).setAll(value).build());
    }

    /**
     * Returns a cron pattern with all indices in some range set to value.
     */
    public CronPattern setRange(TimePeriod period, int low, int high, boolean value) {
        Preconditions.checkNotNull(period, "period argument must be non-null");
        return setInterval(period, new Interval.Builder(getInterval(period)).setRange(low, high, value).build());
    }

    /**
     * Returns a cron pattern with all indices in some increment range set to value.
     */
    public CronPattern setRange(TimePeriod period, int low, int high, int increment, boolean value) {
        Preconditions.checkNotNull(period, "period argument must be non-null");
        return setInterval(period, new Interval.Builder(getInterval(period)).setRange(low, high, increment, value).build());
    }

    /**
     * Returns a cron pattern with one index set to value.
     */
    public CronPattern setIndex(TimePeriod period, int index, boolean value) {
        Preconditions.checkNotNull(period, "period argument must be non-null");
        return setInterval(period, new Interval.Builder(getInterval(period)).setIndex(index, value).build());
    }

    /**
     * Return true if the dayOfWeek, month, and dayOfMonth fields
     * cannot generate a valid date.
     */
    private boolean invalidDays() {
        if (!dayOfWeek.isFull() || month.isFull() || dayOfMonth.isFull()) {
            return false;
        } else {
            Iterator<Integer> monthIterator = month.indexIterator();
            while (monthIterator.hasNext()) {
                int currentMonth = monthIterator.next();
                Iterator<Integer> dayIterator = dayOfMonth.indexIterator();
                while (dayIterator.hasNext()) {
                    int currentDay = dayIterator.next();
                    try {
                        MonthDay.of(currentMonth, currentDay);
                        return false;
                    } catch (DateTimeException ignored) {}
                }
            }
            return true;
        }
    }

    private boolean calculateIsEmpty() {
        if (minute.isEmpty() || hour.isEmpty() || month.isEmpty()) {
            return true;
        } else {
            boolean emptyDays = dayOfWeek.isEmpty() && dayOfMonth.isEmpty();
            return (emptyDays || invalidDays());
        }
    }

    /**
     * Returns true of the minute and hour components of a pattern are matching.
     */
    private boolean minuteHourMatches(Temporal candidate) {
        return minute.test(candidate.get(ChronoField.MINUTE_OF_HOUR)) &&
               hour.test(candidate.get(ChronoField.HOUR_OF_DAY));
    }

    /**
     * Returns true if the day-related components of a pattern
     * (month, dayOfMonth, and dayOfWeek) are matching.
     */
    private boolean dayMatches(Temporal candidate) {
        if (!month.test(candidate.get(ChronoField.MONTH_OF_YEAR))) {
            return false;
        } else if (dayOfMonth.isFull() && dayOfWeek.isFull()) {
            return true;
        } else if (dayOfMonth.isFull()) {
            // joda time uses the ISO definitions, where 1 is Monday and 7 is Sunday.
            return dayOfWeek.test(candidate.get(ChronoField.DAY_OF_WEEK) % 7);
        } else if (dayOfWeek.isFull()) {
            return dayOfMonth.test(candidate.get(ChronoField.DAY_OF_MONTH));
        } else {
            return dayOfWeek.test(candidate.get(ChronoField.DAY_OF_WEEK) % 7) ||
                   dayOfMonth.test(candidate.get(ChronoField.DAY_OF_MONTH));
        }
    }

    /**
     * Returns the next time this pattern will fire if-and-only-if
     * the next time is one the same day as the input day. Otherwise
     * return null.
     */
    private Temporal nextSameDay(Temporal input, boolean inclusive) {
        Temporal output = input;
        if (!dayMatches(output)) {
            return null;
        }
        Integer nextMinute = minute.next(output.get(ChronoField.MINUTE_OF_HOUR), inclusive);
        if (nextMinute != null) {
            output = output.with(ChronoField.MINUTE_OF_HOUR, nextMinute);
        } else {
            output = output.plus(1, ChronoUnit.HOURS);
            if (output.getLong(ChronoField.EPOCH_DAY) != input.getLong(ChronoField.EPOCH_DAY)) {
                return null;
            }
        }
        if (!input.equals(output)) {
            inclusive = true;
        }
        int inputHour = input.get(ChronoField.HOUR_OF_DAY);
        Integer nextHour = hour.next(output.get(ChronoField.HOUR_OF_DAY), inclusive);
        if (nextHour != null) {
            output = output.with(ChronoField.HOUR_OF_DAY, nextHour);
            if (inputHour != nextHour) {
                int newMinute = minute.next(0, true);
                output = output.with(ChronoField.MINUTE_OF_HOUR, newMinute);
            }
            return output;
        } else {
            return null;
        }
    }

    /**
     * Returns the next time this pattern will fire if-and-only-if
     * the previous time is one the same day as the input day. Otherwise
     * return null.
     */
    private Temporal previousSameDay(Temporal input, boolean inclusive) {
        Temporal output = input;
        if (!dayMatches(output)) {
            return null;
        }
        Integer previousMinute = minute.previous(output.get(ChronoField.MINUTE_OF_HOUR), inclusive);
        if (previousMinute != null) {
            output = output.with(ChronoField.MINUTE_OF_HOUR, previousMinute);
        } else {
            output = output.minus(1, ChronoUnit.HOURS);
            if (output.getLong(ChronoField.EPOCH_DAY) != input.getLong(ChronoField.EPOCH_DAY)) {
                return null;
            }
        }
        if (!input.equals(output)) {
            inclusive = true;
        }
        int inputHour = input.get(ChronoField.HOUR_OF_DAY);
        Integer previousHour = hour.previous(output.get(ChronoField.HOUR_OF_DAY), inclusive);
        if (previousHour != null) {
            output = output.with(ChronoField.HOUR_OF_DAY, previousHour);
            if (inputHour != previousHour) {
                int newMinute = minute.previous(59, true);
                output = output.with(ChronoField.MINUTE_OF_HOUR, newMinute);
            }
            return output;
        } else {
            return null;
        }
    }

    /**
     * Find the next temporal match. First test for a match on the same day
     * as the input day. If no match is found on the same day then it is
     * safe to set the hour and minute fields to the first legal values,
     * and then begin iterating through the days.
     */
    private Temporal nextTemporal(Temporal input, boolean inclusive) {
        if (isEmpty()) {
            return null;
        }
        if (inclusive && matches(input)) {
            return input;
        }
        Temporal sameDayOutput = nextSameDay(input, inclusive);
        if (sameDayOutput != null) {
            return sameDayOutput;
        }
        Temporal output = input.plus(1, ChronoUnit.DAYS)
                             .with(ChronoField.HOUR_OF_DAY, hour.next(0, true))
                             .with(ChronoField.MINUTE_OF_HOUR, minute.next(0, true));
        while (true) {
            assert(minuteHourMatches(output));
            if (dayMatches(output)) {
                return output;
            } else {
                output = output.plus(1, ChronoUnit.DAYS);
            }
        }
    }

    /**
     * Find the previous temporal match. First test for a match on the same day
     * as the input day. If no match is found on the same day then it is
     * safe to set the hour and minute fields to the last legal values,
     * and then begin iterating through the days.
     */
    private Temporal previousTemporal(Temporal input, boolean inclusive) {
        Preconditions.checkNotNull(input, "input argument must be non-null");
        if (isEmpty()) {
            return null;
        }
        if (inclusive && matches(input)) {
            return input;
        }
        Temporal sameDayOutput = previousSameDay(input, inclusive);
        if (sameDayOutput != null) {
            return sameDayOutput;
        }
        Temporal output = input.minus(1, ChronoUnit.DAYS)
                       .with(ChronoField.HOUR_OF_DAY, hour.previous(23, true))
                       .with(ChronoField.MINUTE_OF_HOUR, minute.previous(59, true));
        while (true) {
            assert(minuteHourMatches(output));
            if (dayMatches(output)) {
                return output;
            } else {
                output = output.minus(1, ChronoUnit.DAYS);
            }
        }
    }


    /**
     * Date Patterns that run at a specific hour and minute are affected
     * by daylight savings time.
     *
     * @return true if daylight savings correction may be necessary
     */
    private boolean handleZoneTransition() {
        return (!hour.isFull() && !minute.isFull());
    }

    /**
     * If we are in the duplicated time period of an overlap transition,
     * then move forwards around the duplicated time period.
     */
    private ZonedDateTime inputDaylightSavingsNext(ZonedDateTime input) {
        ZoneId zoneId = input.getZone();
        ZoneRules zoneRules = zoneId.getRules();
        ZoneOffset zoneOffset = input.getOffset();
        LocalDateTime localDateTime = input.toLocalDateTime();
        ZoneOffsetTransition transition = zoneRules.getTransition(localDateTime);
        if (transition == null) {
            return input;
        } else if (zoneOffset.equals(transition.getOffsetAfter())) {
            return ZonedDateTime.ofInstant(transition.getDateTimeBefore(),
                                           transition.getOffsetAfter(), zoneId);
        } else {
            return input;
        }
    }

    /**
     * Adjusting the input date time for the previous operation is slightly
     * different from the input adjustments for the next operation. This is
     * because the cron daylight-savings time rules are not commutative.
     * The strategy is to perform any unique adjustments in this function,
     * then compute the local date time previous firing time, and then rely
     * on the same post-calculation daylight savings cleanup that is used by
     * the next operation.
     *
     * If the query is non-inclusive then we must move back a minute to test
     * the input for daylight savings time. If moving back enters into a gap
     * transition then move backwards to before the gap. If we are in the
     * duplicated time period of an overlap transition then move backwards
     * around the duplicated time period.
     */
    private ZonedDateTime inputDaylightSavingsPrevious(ZonedDateTime input, boolean inclusive) {
        ZoneId zoneId = input.getZone();
        ZoneRules zoneRules = zoneId.getRules();
        ZoneOffset zoneOffset = input.getOffset();
        LocalDateTime localDateTime = input.toLocalDateTime();
        if (!inclusive) {
            localDateTime = localDateTime.minusMinutes(1);
        }
        ZoneOffsetTransition transition = zoneRules.getTransition(localDateTime);
        if (transition == null) {
            return input;
        } else if (transition.isGap() || zoneOffset.equals(transition.getOffsetAfter())) {
            return ZonedDateTime.ofInstant(transition.getDateTimeBefore().minusMinutes(1),
                                               transition.getOffsetBefore(), zoneId);
        } else {
            return input;
        }
    }

    /**
     * If there is no daylight savings time transition
     * the ignore any effects of daylight savings and return
     * the local output time.
     *
     * If there is a daylight savings time overlap transition
     * (the clocks are set back) then select the time zone
     * offset from before the transition.
     *
     * If there is a daylight saving time gap transition
     * (the clocks are set forward) then we cannot use the local
     * output time because it is not a legal time value.
     * Move the event to the end of the transition.
     */
    private ZonedDateTime outputAdjustDaylightSavings(LocalDateTime output, ZoneId zoneId) {
        if (output == null) {
            return null;
        }
        ZoneRules zoneRules = zoneId.getRules();
        ZoneOffsetTransition transition = zoneRules.getTransition(output);
        if (transition == null) {
            return ZonedDateTime.of(output, zoneId);
        } else if (transition.isOverlap()) {
            return ZonedDateTime.ofInstant(output, transition.getOffsetBefore(), zoneId);
        } else {
            return ZonedDateTime.ofInstant(transition.getDateTimeAfter(), transition.getOffsetAfter(), zoneId);
        }
    }

}
