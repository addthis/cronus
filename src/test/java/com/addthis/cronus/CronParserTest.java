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

import java.text.ParseException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CronParserTest {

    @Test
    public void parseValidPatterns() throws ParseException {
        CronPattern.build("* * * * *");
        CronPattern.build("1 * * * *");
        CronPattern.build("* */2 * * *");
        CronPattern.build("* * 1-3 * *");
        CronPattern.build("* * * 4-6 *");
        CronPattern.build("* * * * 1-5/3");
        CronPattern.build("* * * * mon,tue,wed,thu,fri");
        CronPattern.build("* * * jan,feb,mar,apr,may,jun,jul,aug,sep,oct,nov,dec *");
        CronPattern.build("* * 4,5,10,15 * *");
        CronPattern.build("* * 4,5,10-17/3,15 * *");
    }

    private void testInvalidPattern(String pattern, int position) {
        boolean failure = false;
        try {
            CronPattern.build(pattern);
        } catch (ParseException ex) {
            assertEquals(position, ex.getErrorOffset());
            failure = true;
        }
        assertTrue("Expected exception was not thrown", failure);
    }

    @Test
    public void parseInvalidPatterns() {
        testInvalidPattern("", 0);
        testInvalidPattern("*", 0);
        testInvalidPattern("* *", 0);
        testInvalidPattern("* * * *", 0);
        testInvalidPattern("* * * * * *", 0);
        testInvalidPattern("* * ** * *", 4);
        testInvalidPattern("* * * * foo", 8);
        testInvalidPattern("* * * * 3-1", 8);
        testInvalidPattern("* * * * -1", 8);
        testInvalidPattern("* * * * 8", 8);
    }

    @Test
    public void print() throws Exception {
        assertEquals("* * * * *", CronParser.print(CronParser.parse("* * * * *")));
        assertEquals("1 1 1 1 1", CronParser.print(CronParser.parse("1 1 1 1 1")));
        assertEquals("1-3 1-3 1-3 1-3 1-3", CronParser.print(CronParser.parse("1-3 1-3 1-3 1-3 1-3")));
        assertEquals("1,3,5,7,9 * * * *", CronParser.print(CronParser.parse("1-10/2 * * * *")));
        assertEquals("59 23 31 12 6", CronParser.print(CronParser.parse("59 23 31 12 6")));
    }

}
