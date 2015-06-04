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

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.text.ParseException;

import com.addthis.cronus.internal.Interval;
import com.addthis.cronus.internal.TimePeriod;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

/**
 * Use {@link CronPattern#build(String)} to construct cron patterns.
 */
class CronParser {

    private static Pattern WHITESPACE = Pattern.compile("\\s+");

    private static Pattern RANGE = Pattern.compile("(\\d+)-(\\d+)");

    private static Pattern RANGE_INCREMENT = Pattern.compile("(\\d+)-(\\d+)/(\\d+)");

    private static Pattern NUMBER = Pattern.compile("\\d+");

    private static Joiner COMMA_JOINER = Joiner.on(',');

    private static Joiner SPACE_JOINER = Joiner.on(' ');

    static String print(CronPattern pattern) {
        Preconditions.checkNotNull(pattern, "pattern argument must be non-null");
        List<String> components = new ArrayList<>();
        TimePeriod[] periods = TimePeriod.values();
        for (int i = 0; i < periods.length; i++) {
            TimePeriod period = periods[i];
            Interval interval = pattern.getInterval(period);
            if (interval.isFull()) {
                components.add("*");
            } else {
                List<String> sections = new ArrayList<>();
                int current = period.min;
                while (current <= period.max) {
                    int start, end;
                    while (current <= period.max && !interval.test(current)) {
                        current++;
                    }
                    if (current > period.max) {
                        break;
                    }
                    start = current;
                    while (current <= period.max && interval.test(current)) {
                        current++;
                    }
                    end = current - 1;
                    if (start == end) {
                        sections.add(Integer.toString(start));
                    } else {
                        sections.add(start + "-" + end);
                    }
                }
                components.add(COMMA_JOINER.join(sections));
            }
        }
        return SPACE_JOINER.join(components);
    }

    static CronPattern parse(String pattern) throws ParseException {
        Preconditions.checkNotNull(pattern, "pattern argument must be non-null");
        pattern = pattern.trim();
        String[] components = WHITESPACE.split(pattern);
        TimePeriod[] periods = TimePeriod.values();
        Interval[] intervals = new Interval[periods.length];
        if (components.length != periods.length) {
            int columns = (pattern.length() == 0) ? 0 : components.length;
            throw new ParseException("Expected " + periods.length + " columns. " +
                                     "Found " + columns + " columns", 0);
        }
        pattern = pattern + " "; // shortcut to avoid special handling of last column
        int index = 0, start = 0, end;
        Matcher matcher = WHITESPACE.matcher(pattern);
        while (matcher.find()) {
            end = matcher.start();
            TimePeriod period = periods[index];
            Interval.Builder builder = new Interval.Builder(period.min, period.max);
            String component = pattern.substring(start, end);
            component = period.replaceConstants(component);
            String[] buckets = component.split(",");
            for (String bucket : buckets) {
                if (bucket.startsWith("*")) {
                    if (bucket.indexOf("*", 1) != -1) {
                        throw new ParseException("wildcard syntax error in " +
                                                 period.description + " column", start);
                    } else {
                        bucket = (period.min + "-" + period.max) + bucket.substring(1, bucket.length());
                    }
                }
                Matcher rangeMatcher = RANGE.matcher(bucket);
                Matcher rangeIncrementMatcher = RANGE_INCREMENT.matcher(bucket);
                Matcher numberMatcher = NUMBER.matcher(bucket);
                ParseException parseException = null;
                try {
                    if (rangeMatcher.matches()) {
                        builder.setRange(Integer.parseInt(rangeMatcher.group(1)),
                                         period.substituteEndRange(Integer.parseInt(rangeMatcher.group(2))),
                                         true);
                    } else if (rangeIncrementMatcher.matches()) {
                        builder.setRange(Integer.parseInt(rangeIncrementMatcher.group(1)),
                                         period.substituteEndRange(Integer.parseInt(rangeIncrementMatcher.group(2))),
                                         Integer.parseInt(rangeIncrementMatcher.group(3)),
                                         true);
                    } else if (numberMatcher.matches()) {
                        builder.setIndex(period.substituteValue(Integer.parseInt(bucket)), true);
                    } else {
                        parseException = new ParseException("Unrecognized value in " +
                                                            period.description + " column", start);
                    }
                } catch (Exception ex) {
                    parseException = new ParseException(ex.getMessage() + " in " +
                                                        period.description + " column", start);
                }
                if (parseException != null) {
                    throw parseException;
                }
            }
            intervals[index] = builder.build();
            index++;
            start = matcher.end();
        }
        return new CronPattern(intervals[0], intervals[1], intervals[2], intervals[3], intervals[4], pattern);
    }

}
