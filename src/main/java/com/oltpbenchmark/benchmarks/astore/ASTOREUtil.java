/*
 * Copyright 2020 by OLTPBenchmark Project
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
 *
 */


package com.oltpbenchmark.benchmarks.astore;

import java.util.Random;

public abstract class ASTOREUtil {

    public static final Random rand = new Random();

    /**
     * @returns a random alphabetic string with length in range [minimum_length, maximum_length].
     */
    public static String RandomStringLetter(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, 'A', 26);
    }

    // taken from tpcc.RandomGenerator

    /**
     * @returns a random numeric string with length in range [minimum_length, maximum_length].
     */
    public static String RandomStringNumber(int minimum_length, int maximum_length) {
        return randomString(minimum_length, maximum_length, '0', 10);
    }

    // taken from tpcc.RandomGenerator 
    public static String randomString(int minimum_length, int maximum_length, char base, int numCharacters) {
        int length = RandomInt(minimum_length, maximum_length).intValue();
        byte baseByte = (byte) base;
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; ++i) {
            bytes[i] = (byte) (baseByte + RandomInt(0, numCharacters - 1));
        }
        return new String(bytes);
    }

    public static Integer RandomInt(int minimum, int maximum) {
        return Math.abs(rand.nextInt()) % (maximum - minimum + 1) + minimum;
    }

    public static Double RandomDouble(double minimum, double maximum) {
        return rand.nextDouble() * (maximum - minimum) + minimum;
    }
}
