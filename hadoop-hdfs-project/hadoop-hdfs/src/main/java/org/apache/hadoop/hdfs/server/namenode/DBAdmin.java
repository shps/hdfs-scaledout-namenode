/**
 * 
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**Used for performing administrative functions on the database. 
 * Assumes that a MySQL API server is running in the cluster
 * @author wmalik
 *
 */
public class DBAdmin {

	public static void truncateAllTables(String database) {
		try {
			   Class.forName("com.mysql.jdbc.Driver").newInstance();

			   String ConnectionString="jdbc:mysql://" + "cloud3.sics.se:3307" + "/" + database + "?user=" +
			          "wasif5" + "&password=" + "wasif";
			   System.out.println("Truncating " + database + " on " + ConnectionString);
			   Connection conn = DriverManager.getConnection(ConnectionString);
			   Statement stmt=conn.createStatement();
			   stmt.execute("call trunc_kthfs()");
			}
			catch(SQLException SQLEx) {
			   System.out.println("MySQL error: "+SQLEx.getErrorCode()+
			          " SQLSTATE:" +SQLEx.getSQLState());
			   System.out.println(SQLEx.getMessage());
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		
	}
	
}
