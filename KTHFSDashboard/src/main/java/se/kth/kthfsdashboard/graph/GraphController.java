package se.kth.kthfsdashboard.graph;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import org.primefaces.model.DashboardColumn;
import org.primefaces.model.DashboardModel;
import org.primefaces.model.DefaultDashboardColumn;
import org.primefaces.model.DefaultDashboardModel;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@RequestScoped
public class GraphController implements Serializable {

   private String kthfsInstance;
   private String hostname;
   private String serviceGroup;
   private String service;
   private Date start;
   private Date end;
   private String period;
   private DashboardModel model;
   private List<String> datePeriods = new ArrayList<String>();

   public GraphController() {


      Collections.addAll(datePeriods, "Last hour", "Last 6 hours", "Last 12 hours", "Last 24 hours", "Last week");

      Calendar c = Calendar.getInstance();
      c.setTime(new Date());
      c.add(Calendar.HOUR_OF_DAY, -1);
      start = c.getTime();
      end = new Date();
      
      period = "1h";

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

   public DashboardModel getModel() {

      model = new DefaultDashboardModel();
      DashboardColumn column1 = new DefaultDashboardColumn();
      DashboardColumn column2 = new DefaultDashboardColumn();
      DashboardColumn column3 = new DefaultDashboardColumn();
      
      column1.addWidget("load");
      column2.addWidget("memory");
      column3.addWidget("df");

      column2.addWidget("interface");
      
      model.addColumn(column1);
      model.addColumn(column2);
      model.addColumn(column3);      
      return model;
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

   public List<String> getDatePeriods() {
      return datePeriods;
   }
   
   public String getGraphUrl() throws MalformedURLException {
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

   public String getGraphUrl2() throws MalformedURLException {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("chart_type", "loadall");      
      params.put("start", getStartTime().toString());
      params.put("end", getEndTime().toString());
      params.put("hostname", "ubuntu");
      params.put("plugin", "load");
      params.put("type", "load");

      String url = "rest/collectd/graph?";
      for (Entry<String, String> entry : params.entrySet()) {
         url += entry.getKey() + "=" + entry.getValue() + "&";
      }
      return url;
   }   
   
   public String getGraphUrl3() throws MalformedURLException {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("chart_type", "memoryall");      
      params.put("start", getStartTime().toString());
      params.put("end", getEndTime().toString());
      params.put("hostname", "ubuntu");
      params.put("plugin", "memory");
      params.put("type", "memory");
      String url = "rest/collectd/graph?";
      for (Entry<String, String> entry : params.entrySet()) {
         url += entry.getKey() + "=" + entry.getValue() + "&";
      }
      return url;
   }   

   public String getGraphUrl4() throws MalformedURLException {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("chart_type", "dfall");      
      params.put("start", getStartTime().toString());
      params.put("end", getEndTime().toString());
      params.put("hostname", "ubuntu");
      params.put("plugin", "df");
      params.put("type", "df");
      String url = "rest/collectd/graph?";
      for (Entry<String, String> entry : params.entrySet()) {
         url += entry.getKey() + "=" + entry.getValue() + "&";
      }
      return url;
   }      
   
   
   public String getGraphUrl5() throws MalformedURLException {
      HashMap<String, String> params = new HashMap<String, String>();
      params.put("chart_type", "interfaceall");      
      params.put("start", getStartTime().toString());
      params.put("end", getEndTime().toString());
      params.put("hostname", "ubuntu");
      params.put("plugin", "interface");
      params.put("type", "if_octets");
      String url = "rest/collectd/graph?";
      for (Entry<String, String> entry : params.entrySet()) {
         url += entry.getKey() + "=" + entry.getValue() + "&";
      }
      return url;
   }      
   
}