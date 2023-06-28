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
import org.json.JSONObject;
import java.util.Map;
import java.sql.Statement;

public class ASTOREWorker extends Worker<ASTOREBenchmark> {
    private static final Logger LOG = LoggerFactory.getLogger(ASTOREWorker.class);
    private static long s_id_init = -1;
    /**
     * Each Transaction element provides an TransactionInvoker to create the proper
     * arguments used to invoke the stored procedure
     */
	public class SizeChangePackage {
		public static int categorySize; 
		public static int userSize;
		public static int addressSize;
		public static int productSize;
		public static int orderSize;
		public static int subscriberSize;
		public static int orderDetailSize;
		public static int messageSize;
		SizeChangePackage()
		{	categorySize = 0;
			userSize = 0;
			addressSize = 0;
			productSize = 0;
			orderSize = 0;
			subscriberSize = 0;
			orderDetailSize = 0;
			messageSize = 0;
		}
	};

    private interface TransactionInvoker<T extends Procedure> {
        /**
         * Generate the proper arguments used to invoke the given stored procedure
         *
         * @param subscriberSize
         * @return
         */
        void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration w) throws SQLException;
    }

    /**
     * Set of transactions structs with their appropriate parameters
     */
    public enum Transaction {
        ProfileIdEdit(new TransactionInvoker<ProfileIdEdit>() {
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration configuration) throws SQLException {
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
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration configuration) throws SQLException {
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
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration configuration) throws SQLException {
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
                   pkg.addressSize += ((ProfileAddressAdd) proc).run(
									conn, req_body_password, req_user_Password, req_body_fullName, req_body_streetAddress, req_body_postcode, req_body_city, req_body_country, req_body_phone, req_user_UserID);
               }
        }),
		ProfileAddressDelete(new TransactionInvoker<ProfileAddressDelete>() {
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration configuration) throws SQLException {
                //long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
				if (addressSize == 0)
					return;		
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
				String req_body_password = "";
				String req_user_Password = "";
				Integer req_params_id = ASTOREUtil.RandomInt(1, addressSize);
                   pkg.addressSize -= ((ProfileAddressDelete) proc).run(
									conn, req_body_password, req_user_Password, req_params_id);
               }
        }),
		ProfileAddressEdit(new TransactionInvoker<ProfileAddressEdit>() {
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration configuration) throws SQLException {
                //long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
				if (addressSize == 0)
					return;		
                configuration.updateQueryCount(2);
                configuration.updateTransactionCount(1);
				String req_body_password = "";
				String req_user_Password = "";
				Integer req_params_id = ASTOREUtil.RandomInt(1, addressSize);
				String req_body_fullname = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_streetAddress  = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_postcode = ASTOREUtil.RandomStringNumber(5, 5);
				String req_body_city = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_country  = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_phone  = ASTOREUtil.RandomStringNumber(10, 10);
                   ((ProfileAddressEdit) proc).run(
									conn, req_params_id, req_body_fullname, req_body_streetAddress, req_body_postcode, req_body_city, req_body_country, req_body_phone, req_body_password, req_user_Password, true);
               }
        }),
		RoutesSubscribe(new TransactionInvoker<RoutesSubscribe>() {
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration configuration) throws SQLException {
                //long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
				String req_body_email = ASTOREUtil.RandomStringLetter(5, 5) + "@" + ASTOREUtil.RandomStringLetter(5, 5) + "." + ASTOREUtil.RandomStringLetter(3, 3) ;
                   pkg.subscriberSize += ((RoutesSubscribe) proc).run(
									conn, req_body_email);
               }
        }),
		CheckoutDeliveryNew(new TransactionInvoker<CheckoutDeliveryNew>() {
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration configuration) throws SQLException {
                //long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
				String req_body_fullName = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_email = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_streetAddress  = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_postcode = ASTOREUtil.RandomStringNumber(5, 5);
				String req_body_city = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_country  = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_phone  = ASTOREUtil.RandomStringNumber(10, 10);
				Integer req_user_UserID = ASTOREUtil.RandomInt(1, userSize);
                   pkg.addressSize += ((CheckoutDeliveryNew) proc).run(
									conn, req_body_fullName, req_body_email, req_body_streetAddress, req_body_postcode, req_body_city, req_body_country, req_body_phone, req_user_UserID);
               }
        }),
		CheckoutOrder(new TransactionInvoker<CheckoutOrder>() {
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration configuration) throws SQLException {
                //long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(4);
                configuration.updateTransactionCount(1);
				Integer req_user_UserID = ASTOREUtil.RandomInt(1, userSize);
				Integer req_session_address_AddressID = ASTOREUtil.RandomInt(1, addressSize);
				Double req_session_cartSummary_subTotal = ASTOREUtil.RandomDouble(5, 1000);
				Double req_session_cartSummary_discount = ASTOREUtil.RandomDouble(1, 10);
				Double req_session_cartSummary_shipCost = ASTOREUtil.RandomDouble(5, 20);
				Double req_session_cartSummary_total = req_session_cartSummary_subTotal - req_session_cartSummary_discount + req_session_cartSummary_shipCost;
				int loop_count = ASTOREUtil.RandomInt(1, 10);
				Map<String, Boolean> item_lookup1 = new HashMap<>();
				Map<Integer, Boolean> item_lookup2 = new HashMap<>();
				JSONObject req_session_cart = new JSONObject();
				pkg.orderSize++;
				for (int i = 0; i < loop_count; i++)
				{	String productName;
					Integer productID;	
					while (true)
					{	productName = ASTOREUtil.RandomStringLetter(5, 5);
						if (item_lookup1.get(productName) == null)
						{	item_lookup1.put(productName, true);
							break;
						}
					}
					while (true)
					{	productID = ASTOREUtil.RandomInt(1, productSize);
						if (item_lookup2.get(productID) == null)
						{	item_lookup2.put(productID, true);
							break;
						}
					}
					JSONObject productData = new JSONObject();
					productData.put("ProductID", productID);
					productData.put("quantity", ASTOREUtil.RandomInt(1, 10));
					productData.put("productTotal", ASTOREUtil.RandomDouble(1, 1000));
					req_session_cart.put(productName, productData);
					configuration.updateQueryCount(2);
					pkg.orderDetailSize++;
				} 
                    ((CheckoutOrder) proc).run(conn, req_user_UserID, req_session_address_AddressID, req_session_cartSummary_subTotal, req_session_cartSummary_discount, req_session_cartSummary_shipCost, req_session_cartSummary_total, req_session_cart.toString() );
               }
        }),
		ContactUs(new TransactionInvoker<ContactUs>() {
            public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration configuration) throws SQLException {
                //long s_id = ASTOREUtil.getSubscriberId(subscriberSize);
                configuration.updateQueryCount(1);
                configuration.updateTransactionCount(1);
				String req_body_fullName = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_email = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_subject  = ASTOREUtil.RandomStringLetter(5, 5);
				String req_body_contactMessage = ASTOREUtil.RandomStringNumber(10, 10);
                pkg.messageSize += ((ContactUs) proc).run(
									conn, req_body_fullName, req_body_email, req_body_subject, req_body_contactMessage);
               }
        }),
        ; // END LIST OF STORED PROCEDURES

        Transaction(TransactionInvoker<? extends Procedure> ag) {
            this.generator = ag;
        }

		public int categorySize = 0;
		public int userSize = 0;
		public int addressSize = 0;
		public int productSize = 0;
		public int orderSize = 0;
		public int subscriberSize = 0;
		public int messageSize = 0;
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

        public void invoke(Connection conn, Procedure proc, int categorySize, int userSize, int addressSize, int productSize, int orderSize, SizeChangePackage pkg, WorkloadConfiguration c) throws SQLException {
            this.generator.invoke(conn, proc, categorySize, userSize, addressSize, productSize, orderSize, pkg, c);
        }
    }

	private int categorySize;
	private int userSize;
	private int addressSize;
	private int productSize;
	private int orderSize;
	private int subscriberSize;
	private int orderDetailSize;
	private int messageSize;
	private int TABLE_TRUNCATE_SIZE;
    public ASTOREWorker(ASTOREBenchmark benchmarkModule, int id) {
        super(benchmarkModule, id);
        this.categorySize = (int)Math.round(ASTOREConstants.DEFAULT_NUM_CATEGORY * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.userSize = (int)Math.round(ASTOREConstants.DEFAULT_NUM_USER * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.addressSize = (int)Math.round(ASTOREConstants.DEFAULT_NUM_ADDRESS * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.productSize = (int)Math.round(ASTOREConstants.DEFAULT_NUM_PRODUCT * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
        this.orderSize = (int)Math.round(ASTOREConstants.DEFAULT_NUM_ORDER * benchmarkModule.getWorkloadConfiguration().getScaleFactor());
		this.subscriberSize = 0;
		this.orderDetailSize = 0;
		this.messageSize = 0;
		this.TABLE_TRUNCATE_SIZE = 200000; //this.userSize * 10;
    }

    @Override
    protected TransactionStatus executeWork(Connection conn, TransactionType txnType) throws UserAbortException, SQLException {
        Transaction t = Transaction.get(txnType.getName());


        // Get the Procedure handle
        Procedure proc = this.getProcedure(txnType);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Executing {}", proc);
        }
		SizeChangePackage pkg = new SizeChangePackage();
        t.invoke(conn, proc, categorySize, userSize, addressSize, productSize, orderSize, pkg, configuration);
		this.categorySize += pkg.categorySize;
		this.userSize += pkg.userSize;
		this.addressSize += pkg.addressSize;
		this.productSize += pkg.productSize;
		this.orderSize += pkg.orderSize;
		this.subscriberSize += pkg.subscriberSize;
		this.orderDetailSize += pkg.orderDetailSize;
		this.messageSize += pkg.messageSize;

		if (this.addressSize >= this.TABLE_TRUNCATE_SIZE)
		{	TruncateTable(conn, "Addresses");
		}
		if (this.orderSize >= this.TABLE_TRUNCATE_SIZE || this.orderDetailSize >= this.TABLE_TRUNCATE_SIZE)
		{	TruncateTable(conn, "Orders");
			TruncateTable(conn, "`Order Details`");
		}
		if (this.subscriberSize >= this.TABLE_TRUNCATE_SIZE)
		{	TruncateTable(conn, "Subscribers");
		}
		if (this.messageSize >= this.TABLE_TRUNCATE_SIZE)
		{	TruncateTable(conn, "Messages");
		}

        return (TransactionStatus.SUCCESS);
    }
	protected void TruncateTable(Connection conn, String tablename)
	{

	   try(Statement stmt = conn.createStatement()) {
		  stmt.executeQuery("SET FOREIGN_KEY_CHECKS = 0");
	   } catch (SQLException ex) { throw new RuntimeException(ex); }

	   try(Statement stmt = conn.createStatement()) {
		  stmt.executeQuery("TRUNCATE TABLE " + tablename);
	   } catch (SQLException ex) { throw new RuntimeException(ex); }
		if (tablename.equals("Addresses"))
		{
		   try(Statement stmt = conn.createStatement()) {
			  stmt.executeQuery("ALTER TABLE Addresses AUTO_INCREMENT = 1");
		   } catch (SQLException ex) { throw new RuntimeException(ex); }

		}
	   try(Statement stmt = conn.createStatement()) {
		  stmt.executeQuery("SET FOREIGN_KEY_CHECKS = 1");
	   } catch (SQLException ex) { throw new RuntimeException(ex); }
	
	}
}
