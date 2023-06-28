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


package com.oltpbenchmark.benchmarks.astore.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.astore.ASTOREConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class ProfileAddressEdit extends Procedure {
/*
    public final SQLStmt getSubscriber = new SQLStmt(
            "SELECT s_id INTO @s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"
    );

    public final SQLStmt updateSubscriber = new SQLStmt(
            "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET vlr_location = ? WHERE s_id = @s_id"
    );
*/
    public final SQLStmt my_procedure = new SQLStmt(
            "CALL ProfileAddressEdit(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );
/*
    public final SQLStmt my_query = new SQLStmt(
            "UPDATE Users SET Fullname = ?, Email = ?, StreetAddress = ?, PostCode = ?, City = ?, Country = ?, Phone = ? WHERE UserID = ?"
    );
*/
    public long run(Connection conn, int req_params_id, String req_body_fullname, String req_body_streetAddress, String req_body_postcode, String req_body_city, String req_body_country, String req_body_phone, String req_body_password, String req_user_Password, boolean compareSync) throws SQLException {

		/* Password Check */	
		if (!req_body_password.equals(req_user_Password))
			return -1;

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, my_procedure, req_params_id, req_body_fullname, req_body_streetAddress, req_body_postcode, req_body_country, req_body_city, req_body_phone, req_body_password, req_user_Password, compareSync)) {
            preparedStatement.execute();
        }
/*
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getSubscriber)) {
            stmt.setString(1, sub_nbr);
            stmt.executeQuery();
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            PreparedStatement stmt2 = this.getPreparedStatement(conn, updateSubscriber);
            stmt2.setInt(1, location);
            //stmt2.setLong(2, s_id);
            return stmt2.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }

*/
        return 0;
    }
}
