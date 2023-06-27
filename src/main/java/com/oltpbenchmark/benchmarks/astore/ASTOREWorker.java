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
        void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, WorkloadConfiguration w) throws SQLException;
    }

    /**
     * Set of transactions structs with their appropriate parameters
     */
    public enum Transaction {
        ProfileIdEdit(new TransactionInvoker<ProfileIdEdit>() {
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, WorkloadConfiguration configuration) throws SQLException {
                //long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
				String req_body_password = "";
				String req_user_Password = "";
				String req_body_fullName = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_email = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_streetAddress  = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_postcode = ASTOREUtil.RandomStringNumber(5, 5);
				String req_body_city = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_country  = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_phone  = ASTOREUtil.RandomStringNumber(10, 10);
				Integer req_user_UserID = ASTOREUtil.RandomInt(1, userSize);
                   ((ProfileIdEdit) proc).run(
									conn, req_body_password, req_user_Password, req_body_fullName, req_body_email, req_body_streetAddress, req_body_postcode, req_body_city, req_body_country, req_body_phone, req_user_UserID);
               }
        }),
        ProfileIdChangePassword(new TransactionInvoker<ProfileIdChangePassword>() {
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, WorkloadConfiguration configuration) throws SQLException {
                //long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
				String req_body_password = "";
				String req_user_Password = "";
				String req_body_newPassword = ASTOREUtil.RandomStringLetter(5, 5);
				Integer req_user_UserID = ASTOREUtil.RandomInt(1, userSize);
                   ((ProfileIdChangePassword) proc).run(
									conn, req_body_password, req_user_Password, req_body_newPassword, req_user_UserID);
               }
        }),
		ProfileAddressAdd(new TransactionInvoker<ProfileAddressAdd>() {
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, WorkloadConfiguration configuration) throws SQLException {
                //long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
				String req_body_password = "";
				String req_user_Password = "";
				String req_body_fullName = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_streetAddress  = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_postcode = ASTOREUtil.RandomStringNumber(5, 5);
				String req_body_city = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_country  = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_phone  = ASTOREUtil.RandomStringNumber(10, 10);
				Integer req_user_UserID = ASTOREUtil.RandomInt(1, userSize);
                   ((ProfileAddressAdd) proc).run(
									conn, req_body_password, req_user_Password, req_body_fullName, req_body_streetAddress, req_body_postcode, req_body_city, req_body_country, req_body_phone, req_user_UserID);
               }
        }),
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

        public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, WorkloadConfiguration c) throws SQLException {
            this.generator.invoke(conn, proc, categorySize, userSize, addressSize, productSize, orderSize, c);
        }
    }

    private final int categorySize;
    private final int userSize;
    private final int addressSize;
    private final int productSize;
    private final int orderSize;

    public ASTOREWorker(ASTOREBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.categorySize = (int)Math.round(ASTOREConstants.DEFAULT_NUM_CATEGORY * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.userSize = (int)Math.round(ASTOREConstants.DEFAULT_NUM_USER * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.addressSize = (int)Math.round(ASTOREConstants.DEFAULT_NUM_ADDRESS * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.productSize = (int)Math.round(ASTOREConstants.DEFAULT_NUM_PRODUCT * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.orderSize = (int)Math.round(ASTOREConstants.DEFAULT_NUM_ORDER * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
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
