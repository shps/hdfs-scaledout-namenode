package se.kth.kthfsdashboard.util;

import java.awt.Color;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
public class RrdtoolCommand {

   private static int DEFAULT_WIDTH = 300;
   private static int DEFAULT_HEIGHT = 150;
   private static int DEFAULT_LOWERLIMIT = 0;
   private static boolean SHOW_DETAILS = false;
   private List<String> cmds;
   private List<String> graphCommands;
   private String hostname;
   private String plugin;
   private String pluginInstance;
   private String type;
   private int start;
   private int end;
   private int width;
   private int height;
   private int lowerLimit;
   private String watermark;
   private String title;
   private String verticalLabel;

   public enum ChartType {

      LINE, AREA, AREA_STACK
   }

   public RrdtoolCommand(String hostname, String plugin, String pluginInstance,
           String type, int start, int end) {
      this.hostname = hostname;
      this.plugin = plugin;
      this.pluginInstance = pluginInstance;
      this.type = type;
      this.start = start;
      this.end = end;
      this.width = DEFAULT_WIDTH;
      this.height = DEFAULT_HEIGHT;
      this.lowerLimit = DEFAULT_LOWERLIMIT;
      cmds = new ArrayList<String>();
      graphCommands = new ArrayList<String>();
   }

   public void setWatermark(String watermark) {
      this.watermark = watermark;
   }

   public void setTitle(String title) {
      this.title = title;
   }

   public void setVerticalLabel(String verticalLabel) {
      this.verticalLabel = verticalLabel;
   }

   public void setLowerLimit(int lowerLimit) {
      this.lowerLimit = lowerLimit;
   }

   public void setGraphSize(int width, int height) {
      this.width = width;
      this.height = height;
   }

   public List<String> getCommands() {
      cmds = new ArrayList<String>();
      cmds.add("rrdtool");
      cmds.add("graph");
      cmds.add("");
      cmds.add("--slope-mode");
      cmds.add("--imgformat=PNG");
      cmds.add("--start=" + start);
      cmds.add("--end=" + end);
      cmds.add("--rigid");
      cmds.add("--height=" + height);
      cmds.add("--width=" + width);
      cmds.add("--lower-limit=" + lowerLimit);
      if (title != null) {
         cmds.add("--title=" + title);
      }
      if (verticalLabel != null) {
         cmds.add("--vertical-label=" + verticalLabel);
      }
      if (watermark != null) {
         cmds.add("--watermark=" + watermark);
      }
      cmds.add("TEXTALIGN:left");
      cmds.addAll(graphCommands);
      return cmds;
   }

   public void addGraph(ChartType chartType, String typeInstance, String ds, String label, String color,
           String detailsFormat) {
      String var = typeInstance + ds;
      String rrdFile = getRrdFileName(hostname, plugin, pluginInstance, type, typeInstance);
      
      Color c = Color.decode("0x"+color).brighter().brighter();
      String brightColor = toHex(c);
      
      graphCommands.add("DEF:" + var + "=" + rrdFile + ":" + ds + ":AVERAGE");
      switch (chartType) {
         case LINE:
            graphCommands.add("LINE1:" + var + "#" + color + ":" + label);
            break;
         case AREA:
            graphCommands.add("AREA:" + var + "#" + brightColor + ":" + label);
            break;
         case AREA_STACK:
            graphCommands.add("AREA:" + var + "#" + brightColor + ":" + label + ":STACK");           
            break;
      }
      if (SHOW_DETAILS && detailsFormat != null) {
         graphCommands.add("GPRINT:" + var + ":AVERAGE:Avg\\:" + detailsFormat);
         graphCommands.add("GPRINT:" + var + ":MIN:Min\\:" + detailsFormat);
         graphCommands.add("GPRINT:" + var + ":MAX:Max\\:" + detailsFormat + "\\l");
      }
   }

   private String getRrdFileName(String hostname, String plugin, String pluginInstance, String type, String typeInstance) {
      String rrdFile = "/var/lib/collectd/rrd/" + hostname;
      rrdFile += "/" + plugin;
      if (pluginInstance != null && !pluginInstance.equals("")) {
         rrdFile += "-" + pluginInstance;
      }
      rrdFile += "/" + type;
      if (typeInstance != null && !typeInstance.equals("")) {
         rrdFile += "-" + typeInstance;
      }
      rrdFile += ".rrd";
      return rrdFile;
   }
   
   private String toHex(Color c) {
      String hex = Integer.toHexString(c.getRGB() & 0xffffff);
      if (hex.length() < 6) {
         hex = "0" + hex;
      }
      return hex;
   }   
}
