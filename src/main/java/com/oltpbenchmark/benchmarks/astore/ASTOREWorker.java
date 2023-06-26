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

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.benchmarks.astore.procedures.*;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.WorkloadConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class ASTOREWorker extends Worker<ASTOREBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(ASTOREWorker.class);
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
        void invoke(Connection conn, Procedure proc, long categorySize, long userSize, long addressSize, long productSize, long orderSize, WorkloadConfiguration w) throws SQLException;
    }

    /**
     * Set of transactions structs with their appropriate parameters
     */
    public enum Transaction {
/*
        ProfileIdEdit(new TransactionInvoker<ProfileIdEdit>() {
            public void invoke(Connection conn, Procedure proc, long categorySize, long userSize, long addressSize, long productSize, long orderSize, WorkloadConfiguration configuration) throws SQLException {
                //long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
                   ((ProfileIdEdit) proc).run(
									conn, 
						 );
               }
            }
        }),
        GetAccessData(new TransactionInvoker<GetAccessData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
                ((GetAccessData) proc).run(
                        conn,
                        s_id, // s_id
                        ASTOREUtil.number(1, 4).byteValue() // ai_type
                );
            }
        }),
        GetNewDestination(new TransactionInvoker<GetNewDestination>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
                ((GetNewDestination) proc).run(
                        conn,
                        s_id, // s_id
                        ASTOREUtil.number(1, 4).byteValue(), // sf_type
                        (byte) (8 * ASTOREUtil.number(0, 2)), // start_time
                        ASTOREUtil.number(1, 24).byteValue() // end_time
                );
            }
        }),
        GetSubscriberData(new TransactionInvoker<GetSubscriberData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                
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
                long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                
                configuration.updateQueryCount(3);
                if (configuration.statedb == 0 || configuration.statedb == 2 && (s_id == s_id_init || s_id_init == -1))
					{	  if (s_id_init == -1) s_id_init = s_id; 
                   configuration.executedQueryCount += 3;
                   configuration.executedTransactionCount += 1;
                   ((InsertCallForwarding) proc).run(
									conn,
									ASTOREUtil.padWithZero(s_id), // sub_nbr
									ASTOREUtil.number(1, 4).byteValue(), // sf_type
									(byte) (8 * ASTOREUtil.number(0, 2)), // start_time
									ASTOREUtil.number(1, 24).byteValue(), // end_time
									ASTOREUtil.padWithZero(s_id) // numberx
						 );
               }
            }
        }),
        UpdateLocation(new TransactionInvoker<UpdateLocation>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(2);
                if (configuration.statedb == 0 || configuration.statedb == 1 && (s_id == 1 || s_id_init == -1))
					{	 if (s_id_init == -1) s_id_init = s_id;
                   configuration.executedQueryCount += 2;
                   configuration.executedTransactionCount += 1;
                   ((UpdateLocation) proc).run(
									conn,
									ASTOREUtil.number(0, Integer.MAX_VALUE).intValue(), // vlr_location
									ASTOREUtil.padWithZero(s_id) // sub_nbr
						 );
               }
            }
        }),
        UpdateSubscriberData(new TransactionInvoker<UpdateSubscriberData>() {
            public void invoke(Connection conn, Procedure proc, long subscriberSize, WorkloadConfiguration configuration) throws SQLException {
                long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
                if (configuration.statedb == 0)
					{	 
                   configuration.executedQueryCount += 2;
                   configuration.executedTransactionCount += 1;
							((UpdateSubscriberData) proc).run(
									conn,
									s_id, // s_id
									ASTOREUtil.number(0, 1).byteValue(), // bit_1
									ASTOREUtil.number(0, 255).shortValue(), // data_a
									ASTOREUtil.number(1, 4).byteValue() // sf_type
						 );
               }
            }
        }),

*/
        ; // END LIST OF STORED PROCEDURES

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

        public void invoke(Connection conn, Procedure proc, long categorySize, long userSize, long addressSize, long productSize, long orderSize, WorkloadConfiguration c) throws SQLException {
            this.generator.invoke(conn, proc, categorySize, userSize, addressSize, productSize, orderSize, c);
        }
    }

    private final long categorySize;
    private final long userSize;
    private final long addressSize;
    private final long productSize;
    private final long orderSize;

    public ASTOREWorker(ASTOREBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.categorySize = Math.round(ASTOREConstants.DEFAULT_NUM_CATEGORY * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.userSize = Math.round(ASTOREConstants.DEFAULT_NUM_USER * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.addressSize = Math.round(ASTOREConstants.DEFAULT_NUM_ADDRESS * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.productSize = Math.round(ASTOREConstants.DEFAULT_NUM_PRODUCT * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.orderSize = Math.round(ASTOREConstants.DEFAULT_NUM_ORDER * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws UserAbortException, SQLException {
        Transaction t = Transaction.get(txnType.getName());


        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing {}", proc);
        }

        t.invoke(conn, proc, categorySize, userSize, addressSize, productSize, orderSize, configuration);
        return (TransactionStatus.SUCCESS);
    }

}
