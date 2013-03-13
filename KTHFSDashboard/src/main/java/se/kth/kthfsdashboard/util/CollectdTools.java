package se.kth.kthfsdashboard.util;

import java.awt.Color;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.stamfest.rrd.CommandResult;
import net.stamfest.rrd.RRDp;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
public class CollectdTools {

   private static final String COLLECTD_PATH = "/var/lib/collectd/rrd/";
   private static final String RRD_EXT = ".rrd";
//    private static final String COLLECTD_LINK = "jarmon/data/";
   private static final String COLLECTD_LINK = COLLECTD_PATH;
   //    public static final String COLLECTD_LIB = "/var/lib/collectd/rrd/";
   Parser parser = new Parser();

   enum ChartType {

      LINE, AREA, AREA_STACK
   }

   public CollectdTools() {
   }

   public Set<String> pluginInstances(String hostName, String plugin) {

      final String p = plugin;
      Set<String> instances = new TreeSet<String>();
      File dir = new File(COLLECTD_PATH + hostName);

      File[] files = dir.listFiles(new FilenameFilter() {
         @Override
         public boolean accept(File dir, String name) {
            return name.startsWith(p.toString());
         }
      });

      if (files == null) {
         return instances;
      }
      for (File file : files) {
         instances.add(file.getName().split("-")[1]);
      }

      return instances;
   }

   public int pluginInstancesCount(String plugin, String hostName) {

      return pluginInstances(plugin, hostName).size();
   }

   public Set<String> typeInstances(String hostName, String plugin) {

      SortedSet<String> instances = new TreeSet<String>();
      File dir = new File(COLLECTD_PATH + hostName + "/" + plugin);

      File[] files = dir.listFiles();

      if (files == null) {
         return instances;
      }
      for (File file : files) {
         instances.add(file.getName().split(RRD_EXT)[0].split("-")[1]);
      }

      return instances;
   }

   public double[] getLastLoad(String hostname) {

      DecimalFormat format = new DecimalFormat("#.##");
      String loads[] = readLastRrdValue(hostname, "load", "", "load", "").split(":")[1].trim().toUpperCase().split(" ");
      double load[] = new double[3];
      try {
         load[0] = format.parse(loads[0]).doubleValue();
         load[1] = format.parse(loads[1]).doubleValue();
         load[2] = format.parse(loads[2]).doubleValue();
      } catch (ParseException ex) {
         Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, ex);
      }
      return load;
   }

   public String getTest(String hostname, String type) {
      return readLastRrdValue(hostname, "memory", "", "memory", type);
   }

   public Long getLatestMemoryStatus(String hostname, String type) {

      String res1 = readLastRrdValue(hostname, "memory", "", "memory", type);
      String result;
      if (res1.lastIndexOf(":") < 1) { // ERROR
         Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, "RRD: " + res1);
         return -1l;
      } else {
         result = res1.split(":")[1].trim().toUpperCase();
      }

      try {
         return parser.parseLong(result);
      } catch (ParseException ex) {

         System.err.println(ex + "- result='" + result + "'");
         Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, ex);
      }
      return null;
   }

   private String readLastRrdValue(String hostname, String plugin, String pluginInstance, String type, String typeInstance) {
      try {
         String pluginURI = pluginInstance.isEmpty() ? plugin : plugin + "-" + pluginInstance;
         String typeURI = typeInstance.isEmpty() ? type : type + "-" + typeInstance;
         RRDp rrd = new RRDp(COLLECTD_LINK + hostname + "/" + pluginURI + "/", "5555");

         //get latest recoded time
         CommandResult result = rrd.command(new String[]{"last", typeURI + RRD_EXT, "MAX"});
         String t = Long.toString(((long) Math.floor((Long.parseLong(result.output.trim()) / 10))) * 10 - 10);
         result = rrd.command(new String[]{"fetch", typeURI + RRD_EXT, "MIN", "-s", t, "-e", t});

         if (!result.ok) {
            System.err.println("ERROR in collectdTools: " + result.error);
            return result.error;
         } else {
            return result.output.split("\\r?\\n")[2];
         }
      } catch (Exception ex) {
         Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, ex);
         return "ERROR";
      }
   }

   private enum GraphType {

      plane, loadall, memoryall, dfall, interfaceall, swapall, 
      nn_capacity, nn_files, nn_load, nn_heartbeats, nn_blockreplication, 
      nn_blocks, nn_specialblocks, nn_datanodes,
      
      nn_r_fileinfo, nn_r_getblocklocations, nn_r_getlisting, nn_r_getlinktarget, nn_r_filesingetlisting,
      nn_w_createfile, nn_w_filescreated, nn_w_createfile_all, nn_w_filesappended, nn_w_filesrenamed, 
      nn_w_deletefile, nn_w_filesdeleted, nn_w_deletefile_all, nn_w_addblock, nn_w_createsymlink,
      nn_o_getadditionaldatanode, nn_o_transactions, nn_o_blockreport, nn_o_syncs, nn_o_transactionsbatchedinsync,
      
      nn_t_fsimageloadtime, nn_t_safemodetime, nn_t_transactionsavgtime, nn_t_syncsavgtime, nn_t_blockreportavgtime;
   }

   public InputStream getGraphStream(String chartType, String hostname, String plugin, String pluginInstance, String type, String typeInstance, String ds, int start, int end) throws IOException {

      String RED = "CB4B4B";
      String BLUE = "AFD8F8";
      String YELLOW = "EDC240";
      String GREEN = "4DA74D";

      int height = 150;
      int width = 290;
      try {
         GraphType.valueOf(chartType);
      } catch (Exception e) {
         chartType = "plane";
      }

      RrdtoolCommand cmd = new RrdtoolCommand(hostname, plugin, pluginInstance, start, end);
      cmd.setGraphSize(width, height);
      
      List<String> namenodes = new ArrayList<String>(Arrays.asList("cloud1.sics.se", "cloud2.sics.se"));
      
      switch (GraphType.valueOf(chartType)) {
         
// - Hosts ---------------------------------------------------------------------         
         case loadall:
            cmd.setTitle("Load");
            cmd.setVerticalLabel(" ");
            cmd.drawLine(type, "", "longterm", "Longterm ", RED, "%5.2lf");
            cmd.drawLine(type, "", "midterm", "Midterm  ", BLUE, "%5.2lf");
            cmd.drawLine(type, "", "shortterm", "Shortterm", YELLOW, "%5.2lf");
            break;

         case memoryall:
            cmd.setTitle("Memory");
            cmd.setVerticalLabel("Byte");
            cmd.drawArea(type, "used", "value", "Used    ", RED, "%5.2lf %S");
            cmd.stackArea(type, "buffered", "value", "Buffered", BLUE, "%5.2lf %S");
            cmd.stackArea(type, "cached", "value", "Cached  ", YELLOW, "%5.2lf %S");
            cmd.stackArea(type, "free", "value", "Free    ", GREEN, "%5.2lf %S");
            break;

         case swapall:
            cmd.setTitle("Swap");
            cmd.setVerticalLabel("Byte");
            cmd.drawArea(type, "used", "value", "Used    ", RED, "%5.2lf %s");
            cmd.stackArea(type, "cached", "value", "Cached  ", YELLOW, "%5.2lf %s");
            cmd.stackArea(type, "free", "value", "Free    ", GREEN, "%5.2lf %s");
            break;

         case dfall:
            cmd.setTitle("Physical Memory");
            cmd.setVerticalLabel("Byte");
            cmd.drawArea(type, "root", "used", "Used", RED, "%5.2lf %S");
            cmd.stackArea(type, "root", "free", "Free", GREEN, "%5.2lf %S");
            break;

         case interfaceall:
            cmd.setTitle("Network Interface");
            cmd.setVerticalLabel("bps");
            cmd.drawLine(type, "eth0", "rx", "RX", YELLOW, "%5.2lf %S");
            cmd.drawLine(type, "eth0", "tx", "TX", BLUE, "%5.2lf %S");
            break;
            
//- Namenode -------------------------------------------------------------------
            
         case nn_capacity:
            cmd.setTitle("Namenode Capacity");
            cmd.setVerticalLabel("GB");
            cmd.setPlugin("GenericJMX-FSNamesystem", "");
            cmd.setHostname("cloud1.sics.se");
            cmd.drawLine("memory-CapacityTotalGB", "", "value", "Total", BLUE, "%5.2lf %S");
            cmd.drawLine("memory-CapacityRemainingGB", "", "value", "Remaining", GREEN, "%5.2lf %S");
            cmd.drawLine("memory-CapacityUsedGB", "", "value", "Used", RED, "%5.2lf %S");
            break;

         case nn_files:
            cmd.setTitle("Files");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-FSNamesystem", "");
            cmd.setHostname("cloud1.sics.se");
            cmd.drawLine("counter-FilesTotal", "", "value", "Total Files", GREEN, "%5.2lf %S");
            break;            

         case nn_load:
            cmd.setTitle("Total Load");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-FSNamesystem", "");            
            cmd.setHostname("cloud1.sics.se");            
            cmd.drawLine("gauge-TotalLoad", "", "value", "Total Load", RED, "%5.2lf %S");
            break;  

         case nn_heartbeats:
            cmd.setTitle("Heartbeats");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-FSNamesystem", "");            
            cmd.setHostname("cloud1.sics.se");            
            cmd.drawLine("counter-ExpiredHeartbeats", "", "value", "Expired Heartbeats", RED, "%5.2lf %S");
            break;  

         case nn_blockreplication:
            cmd.setTitle("Block Replication");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-FSNamesystem", "");            
            cmd.setHostname("cloud1.sics.se");            
            cmd.drawLine("counter-UnderReplicatedBlocks", "", "value", "Under-Replicated", GREEN, "%5.2lf %S");
            cmd.drawLine("counter-PendingReplicationBlocks", "", "value", "Pending", BLUE, "%5.2lf %S");
            cmd.drawLine("counter-ScheduledReplicationBlocks", "", "value", "Scheduled", YELLOW, "%5.2lf %S");            
            break; 
            
         case nn_blocks:
            cmd.setTitle("Blocks");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-FSNamesystem", "");            
            cmd.setHostname("cloud1.sics.se");            
            cmd.drawLine("counter-BlockCapacity", "", "value", "Blocks Total", GREEN, "%5.2lf %S");
            cmd.drawLine("counter-BlocksTotal", "", "value", "Block Capacity", BLUE, "%5.2lf %S");
            break; 
            
         case nn_specialblocks:
            cmd.setTitle("Special Blocks");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-FSNamesystem", "");            
            cmd.setHostname("cloud1.sics.se");            
            cmd.drawLine("counter-CorruptBlocks", "", "value", "Corrupt", RED, "%5.2lf %S");
            cmd.drawLine("counter-ExcessBlocks", "", "value", "Excess", BLUE, "%5.2lf %S");
            cmd.drawLine("counter-MissingBlocks", "", "value", "Missing", YELLOW, "%5.2lf %S");
            cmd.drawLine("counter-PendingDeletionBlocks", "", "value", "Pending Delete", GREEN, "%5.2lf %S");
            break; 
            
         case nn_datanodes:
            cmd.setTitle("Number of Data Nodes");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-FSNamesystemState", "");            
            cmd.setHostname("cloud1.sics.se");            
            cmd.drawLine("gauge-NumDeadDataNodes", "", "value", "Dead", RED, "%5.2lf %S");
            cmd.drawLine("gauge-NumLiveDataNodes", "", "value", "Live", GREEN, "%5.2lf %S");
            break;  
            
//- Namenode W------------------------------------------------------------------            
            
         case nn_r_fileinfo:
            cmd.setTitle("Number of FileInfo operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-FileInfoOps", "", "value", "FileInfo", RED, "%5.2lf %S");
            break;              
            
         case nn_r_getblocklocations:
            cmd.setTitle("Number of GetBlockLocations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-GetBlockLocations", "", "value", "GetBlockLocations", RED, "%5.2lf %S");            
            break;
           
         case nn_r_getlisting:
            cmd.setTitle("Number of GetListing operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-GetListingOps", "", "value", "GetListing", RED, "%5.2lf %S");                
            break;

         case nn_r_getlinktarget:
            cmd.setTitle("Number of GetLinkTarget operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-GetLinkTargetOps", "", "value", "GetLinkTarget", RED, "%5.2lf %S");                
            break;  
            
         case nn_r_filesingetlisting:
            cmd.setTitle("Number of FilesInGetListing operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-FilesInGetListingOps", "", "value", "FilesInGetListing", RED, "%5.2lf %S");                
            break;             

       case nn_w_createfile:
            cmd.setTitle("Number of CreateFile operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-CreateFileOps", "", "value", "CreateFile", RED, "%5.2lf %S");                
            break;   
          
       case nn_w_filescreated:
            cmd.setTitle("Number of Files Created");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-FilesCreated", "", "value", "Files Created", RED, "%5.2lf %S");                
            break;              
          
       case nn_w_createfile_all:
            cmd.setTitle("CreateFile operations"); 
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-CreateFileOps", "", "value", "CreateFile Operations", RED, "%5.2lf %S");                
            cmd.drawSummedLines(namenodes,"counter-FilesCreated", "", "value", "Files Created", BLUE, "%5.2lf %S");  
            break;             

       case nn_w_filesappended:
            cmd.setTitle("Number of Files Appended");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-FilesAppended", "", "value", "Files Appended", RED, "%5.2lf %S");                
            break;                 

       case nn_w_filesrenamed:
            cmd.setTitle("Number of Files Renamed");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-FilesRenamed", "", "value", "Files Renamed", RED, "%5.2lf %S");                
            break;          
          
       case nn_w_deletefile:
            cmd.setTitle("Number of Delete File Operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-DeleteFileOps", "", "value", "DeleteFile", RED, "%5.2lf %S");                
            break;           
          
       case nn_w_filesdeleted:
            cmd.setTitle("Number of Files Deleted");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-FilesDeleted", "", "value", "Files Deleted", RED, "%5.2lf %S");                
            break;          
          
       case nn_w_deletefile_all:
            cmd.setTitle("Delete File Operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-DeleteFileOps", "", "value", "DeleteFile Operations", RED, "%5.2lf %S");   
            cmd.drawSummedLines(namenodes,"counter-FilesDeleted", "", "value", "Files Deleted", BLUE, "%5.2lf %S");                
            break;            
          
       case nn_w_addblock:
            cmd.setTitle("AddBlock Operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-AddBlockOps", "", "value", "AddBlock Operations", RED, "%5.2lf %S");   
            break;  

       case nn_w_createsymlink:
            cmd.setTitle("CreateSymlink Operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");            
            cmd.drawSummedLines(namenodes,"counter-CreateSymlinkOps", "", "value", "CreateSymlink Operations", RED, "%5.2lf %S");   
            break;            
                    
       case nn_o_getadditionaldatanode:
            cmd.setTitle("GetAdditionalDatanode Operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");
            cmd.drawSummedLines(namenodes,"counter-GetAdditionalDatanodeOps", "", "value", "GetAdditionalDatanode Operations", RED, "%5.2lf %S");
            break;
          
       case nn_o_transactions:
            cmd.setTitle("Number of Transaction Operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");
            cmd.drawSummedLines(namenodes,"counter-TransactionsNumOps", "", "value", "Transactions Operations", RED, "%5.2lf %S");
            break;          
          
       case nn_o_transactionsbatchedinsync:
            cmd.setTitle("Number of Transactions Batched In Sync");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");
            cmd.drawSummedLines(namenodes,"counter-TransactionsBatchedInSync", "", "value", "Transactions Batched In Sync", RED, "%5.2lf %S");
            break;          

       case nn_o_blockreport:
            cmd.setTitle("Number of BlockReport Operations");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");
            cmd.drawSummedLines(namenodes,"counter-BlockReportNumOps", "", "value", "BlockReport Operations", RED, "%5.2lf %S");
            break; 

       case nn_o_syncs:
            cmd.setTitle("Number of Syncs");
            cmd.setVerticalLabel(" ");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");
            cmd.drawSummedLines(namenodes,"counter-SyncsNumOps", "", "value", "Syncs", RED, "%5.2lf %S");
            break;           

       case nn_t_blockreportavgtime:
            cmd.setTitle("BlockReport Avg Time");
            cmd.setVerticalLabel("?");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");
            cmd.drawSummedLines(namenodes,"gauge-BlockReportAvgTime", "", "value", "BlockReport Avg Time", RED, "%5.2lf %S");
            break;            

       case nn_t_transactionsavgtime:
            cmd.setTitle("Transactions Avg Time");
            cmd.setVerticalLabel("?");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");
            cmd.drawSummedLines(namenodes,"gauge-TransactionsAvgTime", "", "value", "Transactions Avg Time", RED, "%5.2lf %S");
            break;          
          
       case nn_t_syncsavgtime:
            cmd.setTitle("Syncs Avg Time");
            cmd.setVerticalLabel("?");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");
            cmd.drawSummedLines(namenodes,"gauge-SyncsAvgTime", "", "value", "Syncs Avg Time", RED, "%5.2lf %S");
            break;            

       case nn_t_safemodetime:
            cmd.setTitle("SafeMode Time");
            cmd.setVerticalLabel("?");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");
            cmd.drawSummedLines(namenodes,"gauge-SafeModeTime", "", "value", "SafeMode Time", RED, "%5.2lf %S");
            break;             

       case nn_t_fsimageloadtime:
            cmd.setTitle("FsImage Load Time");
            cmd.setVerticalLabel("?");
            cmd.setPlugin("GenericJMX-NameNodeActivity", "");
            cmd.drawSummedLines(namenodes,"gauge-FsImageLoadTime", "", "value", "FsImage Load Time", RED, "%5.2lf %S");
            break;            
          
         default:
            cmd.setTitle(plugin);
            cmd.setVerticalLabel(plugin);
            cmd.drawLine(type, typeInstance, ds, typeInstance, GREEN, null);
      }

      System.err.println();
      for (String s : cmd.getCommands()) {
         System.err.println(s);
      }

      Process process = new ProcessBuilder(cmd.getCommands()).directory(new File("/usr/bin/"))
              .redirectErrorStream(true).start();
      try {
         process.waitFor();
      } catch (InterruptedException ex) {
         Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, ex);
         return null;
      }
      return process.getInputStream();
   }
}
