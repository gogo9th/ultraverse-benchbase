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

public class ProfileIdChangePassword extends Procedure {
/*
    public final SQLStmt getSubscriber = new SQLStmt(
            "SELECT s_id INTO @s_id FROM " + TATPConstants.TABLENAME_SUBSCRIBER + " WHERE sub_nbr = ?"
    );

    public final SQLStmt updateSubscriber = new SQLStmt(
            "UPDATE " + TATPConstants.TABLENAME_SUBSCRIBER + " SET vlr_location = ? WHERE s_id = @s_id"
    );
*/
/*
    public final SQLStmt my_procedure = new SQLStmt(
            "CALL ProfileIdEdit(?, ?)"
    );
*/
    public final SQLStmt my_query = new SQLStmt(
            "UPDATE Users SET Password = ? WHERE UserID = ?"
    );

    public long run(Connection conn, String req_body_password, String req_user_Password, String req_body_newPassword, Integer req_user_UserID) throws SQLException {

		/* Password Check */	
		if (!req_body_password.equals(req_user_Password))
			return -1;

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, my_query, req_body_newPassword,req_user_UserID)) {
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
