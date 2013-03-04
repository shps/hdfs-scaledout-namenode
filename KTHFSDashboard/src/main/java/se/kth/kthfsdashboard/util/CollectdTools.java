package se.kth.kthfsdashboard.util;

import java.awt.Color;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.text.ParseException;
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

      System.err.println("$");


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

      plane, loadall, memoryall, dfall, interfaceall;
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

      RrrtoolCommand cmd = new RrrtoolCommand(hostname, plugin, pluginInstance, type, start, end);
      switch (GraphType.valueOf(chartType)) {
         case loadall:
            cmd.setGraphSize(width, height);
            cmd.setTitle("Load");
            cmd.addGraph(RrrtoolCommand.ChartType.LINE, "", "longterm", "Longterm", RED, null);
            cmd.addGraph(RrrtoolCommand.ChartType.LINE, "", "midterm", "Midterm", BLUE, null);
            cmd.addGraph(RrrtoolCommand.ChartType.LINE, "", "shortterm", "Shortterm", YELLOW, null);
            break;

         case memoryall:
            cmd.setGraphSize(width, height);
            cmd.setTitle("Memory");
            cmd.setVerticalLabel("Byte");
            cmd.addGraph(RrrtoolCommand.ChartType.AREA, "used", "value", "used", RED, "%5.2lf %S");
            cmd.addGraph(RrrtoolCommand.ChartType.AREA_STACK, "buffered", "value", "buffered", BLUE, "%5.2lf %S");
            cmd.addGraph(RrrtoolCommand.ChartType.AREA_STACK, "cached", "value", "cached", YELLOW, "%5.2lf %S");
            cmd.addGraph(RrrtoolCommand.ChartType.AREA_STACK, "free", "value", "free", GREEN, "%5.2lf %S");
            break;

         case dfall:
            cmd.setGraphSize(width, height);
            cmd.setTitle("Physical Memory");
            cmd.setVerticalLabel("Byte");
            cmd.addGraph(RrrtoolCommand.ChartType.AREA, "root", "used", "used", RED, "%5.2lf %S");
            cmd.addGraph(RrrtoolCommand.ChartType.AREA_STACK, "root", "free", "free", GREEN, "%5.2lf %S");
            break;

         case interfaceall:
            cmd.setGraphSize(width, height);
            cmd.setTitle("Network Interface");
            cmd.setVerticalLabel("bps");
            cmd.addGraph(RrrtoolCommand.ChartType.LINE, "eth0", "rx", "RX", YELLOW, "%5.2lf %S");
            cmd.addGraph(RrrtoolCommand.ChartType.LINE, "eth0", "tx", "TX", BLUE, "%5.2lf %S");
            break;

         default:
            cmd.setTitle(plugin);
            cmd.setVerticalLabel(plugin);
            cmd.addGraph(RrrtoolCommand.ChartType.LINE, typeInstance, ds, typeInstance, GREEN, null);
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
