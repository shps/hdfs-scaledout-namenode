package se.kth.kthfsdashboard.mysql;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
import java.io.InputStream;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.primefaces.model.DefaultStreamedContent;
import org.primefaces.model.StreamedContent;
import org.primefaces.model.UploadedFile;
import se.kth.kthfsdashboard.struct.NodesTableItem;
import se.kth.kthfsdashboard.util.CollectdTools;

// TODO: Loads of hard-coded crap here.

public class MySQLAccess implements Serializable {

   final static String DASH_USERNAME = "kthfs";
   final static String DASH_PASSWORD = "kthfs";
   final static String DASH_DATABASE = "kthfs";
   private Connection connect = null;
   private Statement statement = null;
   private ResultSet resultSet = null;

   public List<NodesTableItem> readDataBase() throws InterruptedException {
      List<NodesTableItem> resultList = new ArrayList<NodesTableItem>();
      try {
         
         Class.forName("com.mysql.jdbc.Driver");

         // TODO - Externalize cloud11 into configuration data. Possibly stored 
         // in the local MySQL DB.
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
//      Thread.sleep(2000); // for test
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

   public StreamedContent getBackup() {
      List<String> command = new ArrayList<String>();
      command.add("mysqldump");
      command.add("--single-transaction");
      command.add("-u" + DASH_USERNAME);
      command.add("-p" + DASH_PASSWORD);
      command.add(DASH_DATABASE);
      try {
         Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
         process.waitFor();
         InputStream inputStream = process.getInputStream();
         StreamedContent backupContent = new DefaultStreamedContent(inputStream, "application/sql", "dashboard.sql");
         return backupContent;
      } catch (Exception ex) {
         Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, ex);
         return null;
      }
   }

   public boolean restore(UploadedFile file) {
      List<String> command = new ArrayList<String>();
      command.add("mysql");
      command.add("-u" + DASH_USERNAME);
      command.add("-p" + DASH_PASSWORD);
      command.add(DASH_DATABASE);
      try {
         InputStream inputStream = file.getInputstream();
         Process process = new ProcessBuilder(command).start();
         byte[] bytes = new byte[1024];
         int read;
         while ((read = inputStream.read(bytes)) != -1) {
            process.getOutputStream().write(bytes, 0, read);
         }
         inputStream.close();
         process.getOutputStream().flush();
         process.getOutputStream().close();
         process.waitFor();
         if (process.exitValue() == 0) {
            return true;
         }
         return false;         
      } catch (Exception ex) {
         Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, ex);         
         return false;
      }
   }
}
