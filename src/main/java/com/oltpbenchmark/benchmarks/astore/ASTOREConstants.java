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

public abstract class ASTOREConstants {

    public static final long DEFAULT_NUM_CATEGORY = 1000;
    public static final long DEFAULT_NUM_USER = 10000;
    public static final long DEFAULT_NUM_ADDRESS = 10000;
    public static final long DEFAULT_NUM_PRODUCT = 1000;
    public static final long DEFAULT_NUM_ORDER = 10000;

    //public static final int SUB_NBR_PADDING_SIZE = 15;


    // ----------------------------------------------------------------
    // STORED PROCEDURE EXECUTION FREQUENCIES (0-100)
    // ----------------------------------------------------------------
    public static final int FREQUENCY_ProfileIdEdit = 5;    // Multi
    public static final int FREQUENCY_ProfileIdChangePassword = 1;   // Single
    public static final int FREQUENCY_ProfileAddressAdd = 5;   // Single
    public static final int FREQUENCY_ProfileAddressDelete = 5;   // Single
    public static final int FREQUENCY_ProfileAddressEdit = 5;    // Multi
    public static final int FREQUENCY_RoutesSubscribe = 10;   // Multi
    public static final int FREQUENCY_CheckoutDeliveryNew = 40;    // Single
    public static final int FREQUENCY_CheckoutOrder = 30;    // Single
    public static final int FREQUENCY_ContactUs = 20;    // Single

    // ----------------------------------------------------------------
    // TABLE NAMES
    // ----------------------------------------------------------------
    public static final String TABLENAME_CATEGORIES = "Categories";
    public static final String TABLENAME_USERS = "Users";
    public static final String TABLENAME_ADDRESSES = "Addresses";
    public static final String TABLENAME_PRODUCTS = "Products";
    public static final String TABLENAME_ORDER_DETAILS = "`Order Details`";
    public static final String TABLENAME_MESSAGES = "Messages";
    public static final String TABLENAME_SUBSCRIBERS = "Subscribers";

    public static final String[] TABLENAMES = {
		TABLENAME_CATEGORIES,
		TABLENAME_USERS,
		TABLENAME_ADDRESSES,
		TABLENAME_PRODUCTS,
		TABLENAME_ORDER_DETAILS,
		TABLENAME_MESSAGES,
		TABLENAME_SUBSCRIBERS
    };
}
