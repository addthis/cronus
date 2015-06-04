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

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;

public class CronusDemo {

    public static void main(String[] args) throws Exception {
        CronScheduler scheduler = new CronScheduler.Builder(1).build();
        System.out.println("Scheduling cron pattern");
        Future<?> future = scheduler.schedule(CronPattern.build("* * * * *"),
                                              () -> System.out.println("hello world"), false);
        // scheduled patterns do not execute until scheduler is started up
        scheduler.startUp();
        for(int i = 0; i < 70; i++) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            System.out.print(i + " ");
        }
        future.cancel(false);
        for(int i = 0; i < 50; i++) {
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
            System.out.print(i + " ");
        }
        System.out.println();
        scheduler.shutDown();
    }
}
