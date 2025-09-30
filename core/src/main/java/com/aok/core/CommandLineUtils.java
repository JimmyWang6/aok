/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aok.core;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.kafka.common.utils.Exit;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Helper functions for dealing with command line utilities.
 */
public class CommandLineUtils {

    public static void printErrorAndExit(String message) {
        System.err.println(message);
        Exit.exit(1, message);
    }

    /**
     * Check that exactly one of a set of mutually exclusive arguments is present.
     */
    public static void checkOneOfArgs(OptionParser parser, OptionSet options, OptionSpec<?>... optionSpecs) {
        if (optionSpecs == null || optionSpecs.length == 0) {
            throw new IllegalArgumentException("At least one option must be provided");
        }

        int presentCount = 0;
        for (OptionSpec<?> spec : optionSpecs) {
            if (options.has(spec)) {
                presentCount++;
            }
        }

        if (presentCount != 1) {
            printUsageAndExit(parser, "Exactly one of the following arguments is required: " +
                Arrays.stream(optionSpecs)
                    .map(Object::toString)
                    .collect(Collectors.joining(", ")));
        }
    }

    public static void printUsageAndExit(OptionParser parser, String message) {
        System.err.println(message);
        try {
            parser.printHelpOn(System.err);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Exit.exit(1, message);
    }
}
