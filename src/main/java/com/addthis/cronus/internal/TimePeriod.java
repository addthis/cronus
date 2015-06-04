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

import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import java.time.DayOfWeek;
import java.time.Month;
import java.time.format.TextStyle;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public enum TimePeriod {

    MINUTE(0, 59, "minute"),
    HOUR(0, 23, "hour"),
    DAYOFMONTH(1, 31, "dayOfMonth"),
    MONTH(1, 12, "month"),
    DAYOFWEEK(0, 6, "dayOfWeek"); // 0 or 7 is Sunday

    public final int min;

    public final int max;

    public final String description;

    public final ImmutableMap<Pattern, String> replacements;

    TimePeriod(int min, int max, String description) {
        this.min = min;
        this.max = max;
        this.description = description;
        this.replacements = buildReplacements(description);
    }

    private static ImmutableMap<Pattern, String> buildReplacements(String description) {
        ImmutableMap.Builder<Pattern, String> builder = ImmutableMap.builder();
        switch (description) {
            case "dayOfWeek":
                DayOfWeek[] days = DayOfWeek.values();
                for (int i = 0; i < days.length; i++) {
                    String needle = days[i].getDisplayName(TextStyle.SHORT, Locale.ENGLISH);
                    builder.put(Pattern.compile(Pattern.quote(needle), Pattern.CASE_INSENSITIVE),
                                Integer.toString(i + 1));
                }
                break;
            case "month":
                Month[] months = Month.values();
                for (int i = 0; i < months.length; i++) {
                    String needle = months[i].getDisplayName(TextStyle.SHORT, Locale.ENGLISH);
                    builder.put(Pattern.compile(Pattern.quote(needle), Pattern.CASE_INSENSITIVE),
                                Integer.toString(i + 1));
                }
                break;
        }
        return builder.build();
    }

    public Interval interval() {
        return new Interval.Builder(min, max).build();
    }

    public Interval validate(Interval interval) {
        Preconditions.checkArgument(interval.getMin() == min,
                                    "Expected interval minimum to be {} but was {}",
                                    min, interval.getMin());
        Preconditions.checkArgument(interval.getMax() == max,
                                    "Expected interval maximum to be {} but was {}",
                                    max, interval.getMax());
        return interval;
    }

    // Workaround for two Sundays in day of week specification
    public int substituteEndRange(int input) {
        if ((this == DAYOFWEEK) && (input == 7)) {
            return 6;
        } else {
            return input;
        }
    }

    // Workaround for two Sundays in day of week specification
    public int substituteValue(int input) {
        if ((this == DAYOFWEEK) && (input == 7)) {
            return 0;
        } else {
            return input;
        }
    }

    public String replaceConstants(String input) {
        for (Map.Entry<Pattern,String> replacement : replacements.entrySet()) {
            input = replacement.getKey().matcher(input).replaceAll(replacement.getValue());
        }
        return input;
    }
}
