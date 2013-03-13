package se.kth.kthfsdashboard.graph;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import se.kth.kthfsdashboard.struct.DatePeriod;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@RequestScoped
public class GraphController implements Serializable {

   private String kthfsInstance;
   @ManagedProperty("#{param.hostname}")
   private String hostname;
   private String serviceGroup;
   private String service;
   private Date start;
   private Date end;
   private String period;
   private int numberOfColumns;
   private List<DatePeriod> datePeriods = new ArrayList<DatePeriod>();
   private List<Integer> columns;
   private List<String> hostGraphs;
   private List<String> namenodeGraphs;
   private List<String> namenodeActivitiesGraphs;
   
   public GraphController() {
      
      namenodeGraphs = new ArrayList<String>(Arrays.asList("nn_capacity", "nn_files", 
              "nn_load", "nn_heartbeats", "nn_blockreplication", "nn_blocks", "nn_specialblocks", "nn_datanodes"));

      namenodeActivitiesGraphs = new ArrayList<String>(Arrays.asList("nn_r_fileinfo", "nn_r_getblocklocations", 
              "nn_r_getlisting", "nn_r_getlinktarget", "nn_r_filesingetlisting", "nn_w_createfile_all", 
              "nn_w_filesappended", "nn_w_filesrenamed", "nn_w_deletefile_all", "nn_w_addblock", "nn_w_createsymlink",
              "nn_o_getadditionaldatanode" , "nn_o_transactions", "nn_o_transactionsbatchedinsync", "nn_o_blockreport", "nn_o_syncs",
              "nn_t_fsimageloadtime", "nn_t_safemodetime", "nn_t_transactionsavgtime", "nn_t_syncsavgtime", "nn_t_blockreportavgtime"));     
      
      hostGraphs = new ArrayList<String>(Arrays.asList("load", "memory", "df", "interface", "swap"));
      
      columns = new ArrayList<Integer>(Arrays.asList(2,3,4,5));

      datePeriods.add(new DatePeriod("hour", "1h"));
      datePeriods.add(new DatePeriod("2hr", "2h"));
      datePeriods.add(new DatePeriod("4hr", "4h"));
      datePeriods.add(new DatePeriod("day", "1d"));
      datePeriods.add(new DatePeriod("week", "7d"));
      datePeriods.add(new DatePeriod("month", "1m"));
      datePeriods.add(new DatePeriod("year", "1y"));

      Calendar c = Calendar.getInstance();
      c.setTime(new Date());
      c.add(Calendar.HOUR_OF_DAY, -1);
      start = c.getTime();
      end = new Date();

      period = "1h";
      
      if (numberOfColumns == 0 ){
         numberOfColumns = 4;
      }

   }

   public String getKthfsInstance() {
      return kthfsInstance;
   }

   public void setKthfsInstance(String kthfsInstance) {
      this.kthfsInstance = kthfsInstance;
   }

   public String getHostname() {
      return hostname;
   }

   public void setHostname(String hostname) {
      this.hostname = hostname;
   }

   public String getServiceGroup() {
      return serviceGroup;
   }

   public void setServiceGroup(String serviceGroup) {
      this.serviceGroup = serviceGroup;
   }

   public String getService() {
      return service;
   }

   public void setService(String service) {
      this.service = service;
   }

   public Date getStart() {
      return start;
   }

   public void setStart(Date start) {
      this.start = start;
   }

   public Long getStartTime() {
      return longTime(getStart());
   }

   public Date getEnd() {
      return end;
   }

   public void setEnd(Date end) {
      this.end = end;
   }

   public Long getEndTime() {
      return longTime(getEnd());
   }

   public void updateDates() {

      Calendar c = Calendar.getInstance();
      c.setTime(new Date());
      String unit = period.substring(period.length() - 1);
      int delta = Integer.parseInt(period.substring(0, period.length() - 1));

      if (unit.equals("h")) {
         c.add(Calendar.HOUR_OF_DAY, -delta);
      } else if (unit.equals("d")) {
         c.add(Calendar.DAY_OF_MONTH, -delta);
      } else if (unit.equals("m")) {
         c.add(Calendar.MONTH, -delta);
      } else if (unit.equals("y")) {
         c.add(Calendar.YEAR, -delta);
      } else {
         return;
      }
      start = c.getTime();
      end = new Date();
   }

   public void useCalendar() {
      period = null;
   }

   private Long longTime(Date d) {
      return d.getTime() / 1000;
   }

   public String getPeriod() {
      return period;
   }

   public void setPeriod(String period) {
      this.period = period;
   }

   public List<DatePeriod> getDatePeriods() {
      return datePeriods;
   }
   
   public List<String> getNamenodeGraphs() {
      return namenodeGraphs;
   }

   public String getPlaneGraphUrl() throws MalformedURLException {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("start", getStartTime().toString());
      params.put("end", getEndTime().toString());
      params.put("hostname", "ubuntu");
      params.put("plugin", "memory");
//      params.put("plugin_instance", "memory");
      params.put("type", "memory");
      params.put("type_instance", "used");

      String url = "rest/collectd/graph?";
      for (Entry<String, String> entry : params.entrySet()) {
         url += entry.getKey() + "=" + entry.getValue() + "&";
      }
      return url;
   }

   public String getGraphUrl(String host, String plugin, String type, String chartType) throws MalformedURLException {
      String url = "../rest/collectd/graph?";      
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("chart_type", chartType);
      params.put("start", getStartTime().toString());
      params.put("end", getEndTime().toString());
      params.put("hostname", host);
      params.put("plugin", plugin);
      params.put("type", type);
      for (Entry<String, String> entry : params.entrySet()) {
         url += entry.getKey() + "=" + entry.getValue() + "&";
      }
      return url;
   }

   public String getHostGraphUrl(String plugin) throws MalformedURLException {

      String type;
      if (plugin.equals("interface")) {
         type = "if_octets";
      } else {
         type = plugin;
      }
      return getGraphUrl(hostname, plugin, type, plugin + "all");
   }   
   
   public String getGraphUrl(String host, String plugin, String type) throws MalformedURLException {
//      TODO: host/hostname ?
      return getGraphUrl(hostname, plugin, type, plugin + "all");
   }
   
   public String getNamenodeGraphUrl(String service, String chartType) {
      String url = "../rest/collectd/graph?";
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("chart_type", chartType);
      params.put("start", getStartTime().toString());
      params.put("end", getEndTime().toString());
      for (Entry<String, String> entry : params.entrySet()) {
         url += entry.getKey() + "=" + entry.getValue() + "&";
      }
      return url;
      
   }

   public int getNumberOfColumns() {
      return numberOfColumns;
   }

   public void setNumberOfColumns(int numberOfColumns) {
      this.numberOfColumns = numberOfColumns;
   }

   public List<Integer> getColumns() {
      return columns;
   }

   public void setColumns(List<Integer> columns) {
      this.columns = columns;
   }

   public List<String> getNamenodeActivitiesGraphs() {
      return namenodeActivitiesGraphs;
   }

   public List<String> getHostGraphs() {
      return hostGraphs;
   }


   
}