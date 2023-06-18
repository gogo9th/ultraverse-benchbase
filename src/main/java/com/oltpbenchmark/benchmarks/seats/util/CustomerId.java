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


package com.oltpbenchmark.benchmarks.seats.util;

import com.oltpbenchmark.util.CompositeId;

import java.util.Comparator;
import java.util.Objects;

public class CustomerId /*extends CompositeId*/ implements Comparable<CustomerId> {
/*
    private static final int[] COMPOSITE_BITS = {
            INT_MAX_DIGITS, // ID
            LONG_MAX_DIGITS // AIRPORT_ID
    };
*/
    private int id;
    private long depart_airport_id;
    private String customer_padding, airport_padding;

    public CustomerId(Integer id, Long depart_airport_id, Long max_customer, Integer max_airport) {
        this.id = id;
        this.depart_airport_id = depart_airport_id;
        int customer_padding_length = max_customer.toString().length() - id.toString().length();
        int airport_padding_length = max_airport.toString().length() - depart_airport_id.toString().length();
        this.customer_padding = "";
        this.airport_padding = "";
        for (int i = 0; i < customer_padding_length; i++)
            this.customer_padding += "0";
        for (int i = 0; i < airport_padding_length; i++)
            this.airport_padding += "0";
        //System.out.println(max_customer.toString() + " vs " + max_airport.toString());
        //System.out.println(this.customer_padding +", "+ this.airport_padding + " FOR " + new Integer(id).toString() + ", " + new Long(depart_airport_id).toString());
    }

    public CustomerId(String composite_id, Long max_customer, Integer max_airport) {

        this.decode(composite_id, max_customer, max_airport);

        Integer customer_padding_length = (max_customer.toString().length() - new Integer(this.id).toString().length());
        Integer airport_padding_length = (max_airport.toString().length() - new Long(this.depart_airport_id).toString().length());
        this.customer_padding = "";
        this.airport_padding = "";
        for (int i = 0; i < customer_padding_length; i++)
            this.customer_padding += "0";
        for (int i = 0; i < airport_padding_length; i++)
            this.airport_padding += "0";
    }
    
    //@Override
    public String encode() {
        String id_str = new Integer(this.id).toString();
        String airport_id_str = new Long(this.depart_airport_id).toString();
        
        return this.customer_padding.concat(new Integer(this.id).toString()).concat(this.airport_padding).concat(new Long(this.depart_airport_id).toString());
       // HACK: simpler encoding
        //return (this.encode(COMPOSITE_BITS));
    }

    //@Override
    public void decode(String composite_id, long max_customer, int max_airport) {
        String c_id_str = "";
        String airport_id_str = "";
        int cur_index = 0;
        boolean is_started_customer = false, is_started_airport = false;
        int customer_id_max_length = new Long(max_customer).toString().length();
        int airport_max_length = new Integer(max_airport).toString().length();

        for (int i = 0; i < customer_id_max_length; i++)
        {   if (composite_id.charAt(i) == '0' && !is_started_customer)
                continue;
            else
            {   is_started_customer = true;
                c_id_str += composite_id.charAt(i);
            }
        }
        for (int i = customer_id_max_length; i < customer_id_max_length + airport_max_length; i++)
        {   if (composite_id.charAt(i) == '0' && !is_started_airport)
                continue;
            else
            {   is_started_airport = true;
                airport_id_str += composite_id.charAt(i);
            }
        }
        this.id = Integer.parseInt(c_id_str);
        this.depart_airport_id = Long.parseLong(airport_id_str);
       // HACK: simpler decoding
        /*String[] values = super.decode(composite_id, COMPOSITE_BITS);
        this.id = Integer.parseInt(values[0]);
        this.depart_airport_id = Long.parseLong(values[1]);*/
    }

    //@Override
    public String[] toArray() {
        return (new String[]{Integer.toString(this.id), Long.toString(this.depart_airport_id)});
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @return the depart_airport_id
     */
    public long getDepartAirportId() {
        return depart_airport_id;
    }

    //@Override
    public String toString() {
        return String.format("CustomerId{airport=%d,id=%d}", this.depart_airport_id, this.id);
    }

    //@Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CustomerId that = (CustomerId) o;
        return id == that.id && depart_airport_id == that.depart_airport_id;
    }

    //@Override
    public int hashCode() {
        return Objects.hash(id, depart_airport_id);
    }

    @Override
    public int compareTo(CustomerId o) {
        return Comparator.comparingInt(CustomerId::getId)
                .thenComparingLong(CustomerId::getDepartAirportId)
                .compare(this, o);
    }
}
