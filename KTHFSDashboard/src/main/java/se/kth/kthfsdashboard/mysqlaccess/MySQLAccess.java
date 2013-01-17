package se.kth.kthfsdashboard.mysqlaccess;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import se.kth.kthfsdashboard.struct.NodesTableItem;

public class MySQLAccess {

   private Connection connect = null;
   private Statement statement = null;
   private PreparedStatement preparedStatement = null;
   private ResultSet resultSet = null;

   public List<NodesTableItem> readDataBase() throws InterruptedException {
      List<NodesTableItem> resultList = new ArrayList<NodesTableItem>();

      try {
         Class.forName("com.mysql.jdbc.Driver");
         connect = DriverManager.getConnection("jdbc:mysql://cloud11.sics.se/ndbinfo?"
                 + "user=kthfs&password=kthfs");

         statement = connect.createStatement();
         resultSet = statement.executeQuery("select * from nodes");

         
         while (resultSet.next()) {
            Integer nodeId = resultSet.getInt("node_id");
            String status = resultSet.getString("status");
            Long uptime = resultSet.getLong("uptime");
            Integer startPhase = resultSet.getInt("start_phase");
            Integer configGeneration = resultSet.getInt("config_generation");
            resultList.add(new NodesTableItem(nodeId, status, uptime, startPhase, configGeneration));
         }

      } catch (Exception e) {
            resultList.add(new NodesTableItem(null, "Error" + e.getMessage(), null, null, null));
         
      } finally {
         close();
      }
      
//      Thread.sleep(10000); // for test
      
      return resultList;

   }

   private void close() {
      try {
         if (resultSet != null) {
            resultSet.close();
         }

         if (statement != null) {
            statement.close();
         }

         if (connect != null) {
            connect.close();
         }
      } catch (Exception e) {
      }
   }
}
