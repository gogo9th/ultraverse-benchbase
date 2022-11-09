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


package com.oltpbenchmark.benchmarks.tatp;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.tatp.procedures.*;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.WorkloadConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class TATPWorker extends Worker<TATPBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(TATPWorker.class);
    private static long s_id_init = -1;
    /**
     * Each Transaction element provides an TransactionInvoker to create the proper
     * arguments used to invoke the stored procedure
     */
    private interface TransactionInvoker<T extends Procedure> {
        /**
         * Generate the proper arguments used to invoke the given stored procedure
         *
         * @param subscriberSize
         * @return
         */
        void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration w) throws SQLException;
    }

    /**
     * Set of transactions structs with their appropriate parameters
     */
    public enum Transaction {
        DeleteCallForwarding(new TransactionInvoker<DeleteCallForwarding>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(2);
                configuration.updateTransactionCount(1);
                if (configuration.statedb == 0 || configuration.statedb == 2 && (s_id == s_id_init || s_id_init == -1))
					{	  if (s_id_init == -1) s_id_init = s_id; 
                   configuration.executedQueryCount += 2;
                   configuration.executedTransactionCount += 1;
                   ((DeleteCallForwarding) proc).run(
									conn,
									TATPUtil.padWithZero(s_id), // s_id
									TATPUtil.number(1, 4).byteValue(), // sf_type
									(byte) (8 * TATPUtil.number(0, 2)) // start_time
						 );
               }
            }
        }),
        GetAccessData(new TransactionInvoker<GetAccessData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
                ((GetAccessData) proc).run(
                        conn,
                        s_id, // s_id
                        TATPUtil.number(1, 4).byteValue() // ai_type
                );
            }
        }),
        GetNewDestination(new TransactionInvoker<GetNewDestination>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
                ((GetNewDestination) proc).run(
                        conn,
                        s_id, // s_id
                        TATPUtil.number(1, 4).byteValue(), // sf_type
                        (byte) (8 * TATPUtil.number(0, 2)), // start_time
                        TATPUtil.number(1, 24).byteValue() // end_time
                );
            }
        }),
        GetSubscriberData(new TransactionInvoker<GetSubscriberData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
                ((GetSubscriberData) proc).run(
                        conn,
                        s_id // s_id
                );
            }
        }),
        InsertCallForwarding(new TransactionInvoker<InsertCallForwarding>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                
                configuration.updateQueryCount(3);
                if (configuration.statedb == 0 || configuration.statedb == 2 && (s_id == s_id_init || s_id_init == -1))
					{	  if (s_id_init == -1) s_id_init = s_id; 
                   configuration.executedQueryCount += 3;
                   configuration.executedTransactionCount += 1;
                   ((InsertCallForwarding) proc).run(
									conn,
									TATPUtil.padWithZero(s_id), // sub_nbr
									TATPUtil.number(1, 4).byteValue(), // sf_type
									(byte) (8 * TATPUtil.number(0, 2)), // start_time
									TATPUtil.number(1, 24).byteValue(), // end_time
									TATPUtil.padWithZero(s_id) // numberx
						 );
               }
            }
        }),
        UpdateLocation(new TransactionInvoker<UpdateLocation>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(2);
                if (configuration.statedb == 0 || configuration.statedb == 1 && (s_id == 1 || s_id_init == -1))
					{	 if (s_id_init == -1) s_id_init = s_id;
                   configuration.executedQueryCount += 2;
                   configuration.executedTransactionCount += 1;
                   ((UpdateLocation) proc).run(
									conn,
									TATPUtil.number(0, Integer.MAX_VALUE).intValue(), // vlr_location
									TATPUtil.padWithZero(s_id) // sub_nbr
						 );
               }
            }
        }),
        UpdateSubscriberData(new TransactionInvoker<UpdateSubscriberData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = TATPUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
                if (configuration.statedb == 0)
					{	 
                   configuration.executedQueryCount += 2;
                   configuration.executedTransactionCount += 1;
							((UpdateSubscriberData) proc).run(
									conn,
									s_id, // s_id
									TATPUtil.number(0, 1).byteValue(), // bit_1
									TATPUtil.number(0, 255).shortValue(), // data_a
									TATPUtil.number(1, 4).byteValue() // sf_type
						 );
               }
            }
        }),
        ; // END LIST OF STORED PROCEDURES

        /**
         * Constructor
         */
        Transaction(TransactionInvoker<? extends Procedure> ag) {
            this.generator = ag;
        }

        public final TransactionInvoker<? extends Procedure> generator;

        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<>();
        protected static final Map<String, Transaction> name_lookup = new HashMap<>();

        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
                Transaction.name_lookup.put(vt.name().toUpperCase(), vt);
            }
        }

        public static Transaction get(String name) {
            return (Transaction.name_lookup.get(name.toUpperCase()));
        }

        public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration c) throws SQLException {
            this.generator.invoke(conn, proc, subscriberSize, c);
        }

    }

    private final long subscriberSize;

    public TATPWorker(TATPBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.subscriberSize = Math.round(TATPConstants.DEFAULT_NUM_SUBSCRIBERS * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws UserAbortException, SQLException {
        Transaction t = Transaction.get(txnType.getName());


        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing {}", proc);
        }

        t.invoke(conn, proc, subscriberSize, configuration);
        return (TransactionStatus.SUCCESS);
    }

}
