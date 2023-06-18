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


package com.oltpbenchmark.benchmarks.tpcc;

import java.text.SimpleDateFormat;

public final class TPCCConfig {

    public final static String[] nameTokens = {"BAR", "OUGHT", "ABLE", "PRI",
            "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

    public final static String terminalPrefix = "Term-";

    public final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /* 정상적 세팅값. 최종  테스트시에는이 세팅값을 사용해야 합니다. */
    //public final static int configWhseCount = 1;
    //public final static int configItemCount = 100000; // tpc-c std = 100,000
    //public final static int configDistPerWhse = 10; // tpc-c std = 10
    //public final static int configCustPerDist = 3000; // tpc-c std = 3,000

    /* 디버깅 전용 세팅값. load 소요시간을 90초 내외로 단축하도록 일부 수치를 낮추었습니다. */
    public final static int configWhseCount = 1;
    public final static int configItemCount = 100000; // tpc-c std = 100,000
    public final static int configDistPerWhse = 10; // tpc-c std = 10
    public final static int configCustPerDist = 3000; // tpc-c std = 3,000

    /**
     * An invalid item id used to rollback a new order transaction.
     */
    public static final int INVALID_ITEM_ID = -12345;
}
