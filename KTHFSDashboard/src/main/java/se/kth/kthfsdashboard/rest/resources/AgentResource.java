package se.kth.kthfsdashboard.rest.resources;

import java.util.Date;
import java.util.List;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import se.kth.kthfsdashboard.alert.Alert;
import se.kth.kthfsdashboard.alert.AlertEJB;
import se.kth.kthfsdashboard.host.Host;
import se.kth.kthfsdashboard.host.HostEJB;
import se.kth.kthfsdashboard.service.Service;
import se.kth.kthfsdashboard.service.ServiceEJB;

/**
 * :
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@Path("/agent")
@Stateless
@RolesAllowed({"AGENT", "ADMIN"})
public class AgentResource {

   @EJB
   private HostEJB hostEJB;
   @EJB
   private ServiceEJB serviceEJB;
   @EJB
   private AlertEJB alertEJB;
   
   @GET   
   @Path("ping")
   @Produces(MediaType.TEXT_PLAIN)
   public String getLog() {
      return "KTHFSDashboard: Pong";
   }
   
   @GET
   @Path("load/{name}")   
   @Produces(MediaType.APPLICATION_JSON)
   public Response getLoadAvg(@PathParam("name") String name) {
      Host host = hostEJB.findHostByName(name);
      if (host == null) {
         return Response.status(Status.NOT_FOUND).build();
      }
      JSONObject json = new JSONObject();
      try {
         json.put("hostname", host.getName());
         json.put("cores", host.getCores());
         json.put("load1", host.getLoad1());
         json.put("load5", host.getLoad5());
         json.put("load15", host.getLoad15());
      } catch (Exception ex) {
         return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
      }
      return Response.ok(json).build();
   }

   @GET
   @Path("loads")
   @Produces(MediaType.APPLICATION_JSON)
   public Response getLoads() {
      JSONArray jsonArray = new JSONArray();
      JSONObject json;
      List<Host> hosts = hostEJB.findHosts();
      if (hosts == null) {
         return Response.status(Status.NOT_FOUND).build();
      }
      for (Host host : hosts) {
         try {
            json = new JSONObject();
            json.put("hostname", host.getName());
            json.put("cores", host.getCores());
            json.put("load1", host.getLoad1());
            json.put("load5", host.getLoad5());
            json.put("load15", host.getLoad15());
            jsonArray.put(json);
         } catch (Exception ex) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
         }
      }
      return Response.ok(jsonArray).build();
   }

   @POST
   @Path("/keep-alive")
   @Consumes(MediaType.APPLICATION_JSON)
   public Response keepAlive(@Context HttpServletRequest req, String jsonStrig) {
      try {
         JSONObject json = new JSONObject(jsonStrig);
         long agentTime = json.getLong("agent-time");

         Host host = new Host();
         host.setLastHeartbeat((new Date()).getTime());
         host.setName(json.getString("hostname"));
         host.setIp(json.getString("ip"));
         host.setLoad1(json.getDouble("load1"));
         host.setLoad5(json.getDouble("load5"));
         host.setLoad15(json.getDouble("load15"));
         host.setDiskCapacity(json.getLong("disk-capacity"));
         host.setDiskUsed(json.getLong("disk-used"));
         host.setMemoryCapacity(json.getLong("memory-capacity"));
         host.setMemoryUsed(json.getLong("memory-used"));

         if (!json.isNull("init")) {
            host.setCores(json.getInt("cores"));
            host.setRack(json.getString("rack"));
            hostEJB.storeHost(host, true);
         } else {
            hostEJB.storeHost(host, false);
         }

         JSONArray servicesArray = json.getJSONArray("services");

         for (int i = 0; i < servicesArray.length(); i++) {

            JSONObject s = servicesArray.getJSONObject(i);
            Service service = new Service();
            service.setHostname(host.getName());
            service.setService(s.getString("service"));
            service.setServiceGroup(s.getString("service-group"));
            service.setInstance(s.getString("instance"));
            if (s.has("web-port")) {
               service.setWebPort(s.getInt("web-port"));
            }
            service.setPid(s.has("pid") ? s.getInt("pid") : 0);
            if (s.has("stop-time")) {
               service.setUptime(s.getLong("stop-time") - s.getLong("start-time"));
            } else if (s.has("start-time")) {
               service.setUptime(agentTime - s.getLong("start-time"));
            }
            service.setStatus(Service.Status.valueOf(s.getString("status")));
            serviceEJB.storeService(service);
         }
      } catch (Exception ex) {
         System.err.println("Exception: " + ex);
         return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
      }
      return Response.ok("OK").build();
   }

   @POST
   @Path("/alert")
   @Consumes(MediaType.APPLICATION_JSON)
   public Response alert(@Context HttpServletRequest req, String jsonStrig) {

//       TODO: Alerts are stored in the database. Later, we should define reactions (Email, SMS, ...).
      try {
         JSONObject json = new JSONObject(jsonStrig);

         Alert alert = new Alert();
         alert.setAlertTime(new Date());

         alert.setAgentTime(json.getLong("Time"));
         alert.setMessage(json.getString("Message"));
         alert.setHostname(json.getString("Host"));
         alert.setSeverity(Alert.Severity.valueOf(json.getString("Severity")));

         alert.setPlugin(json.getString("Plugin"));
         if (json.has("PluginInstance")) {
            alert.setPluginInstance(json.getString("PluginInstance"));
         }

         alert.setType(json.getString("Type"));
         alert.setTypeInstance(json.getString("TypeInstance"));

         alert.setDataSource(json.getString("DataSource"));
         alert.setCurrentValue(json.getString("CurrentValue"));
         alert.setWarningMin(json.getString("WarningMin"));
         alert.setWarningMax(json.getString("WarningMax"));
         alert.setFailureMin(json.getString("FailureMin"));
         alert.setFailureMax(json.getString("FailureMax"));

         alertEJB.persistAlert(alert);

      } catch (Exception ex) {
         System.err.println("Exception: " + ex);
         return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
      }
      return Response.ok().build();
   }
}
