package se.kth.kthfsdashboard.rest.resources;

import javax.annotation.security.RolesAllowed;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import se.kth.kthfsdashboard.log.LogEJB;
import se.kth.kthfsdashboard.log.TempLogStore;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@Path("/collectd")
@Stateless
@RolesAllowed({"AGENT", "ADMIN"})
public class CollectdResource {

   @EJB
   private LogEJB logEJB;
   
   TempLogStore tempLogStore = new TempLogStore();

   @GET
   @Path("ping")
   @Produces(MediaType.TEXT_PLAIN)
   public String getLog() {
      return "KTHFSDashboard: Pong";
   }

   @POST
   @Consumes(MediaType.APPLICATION_JSON)
   public String postLog(@Context HttpServletRequest Req, String jsonArrayStrig) {
//        try {
//        JSONArray jsonArray = new JSONArray(jsonArrayStrig);
//        for (int i = 0; i < jsonArray.length(); i++) {
//            JSONObject json = jsonArray.getJSONObject(i);
//            long time = json.getLong("time");
//            int interval = json.getInt("interval");
//            String values = json.getString("values");
//            String host = json.getString("host");
//            String plugin = json.getString("plugin");
//            String pluginInstance = json.getString("plugin_instance");
//            String type = json.getString("type");
//            String typeInstance = json.getString("type_instance");
//            Log log = new Log(values, time, interval, host, plugin, pluginInstance, type, typeInstance);
//
//            if (tempLogStore.isPluginTypeValid(log)) {
//                tempLogStore.addLog(log);
//                if (tempLogStore.isTargetCountSatisfied(log)) {
//                    logEJB.addAllLogs(tempLogStore.removeLogs(log));
//                }
////                System.out.println("----------> " + tempLogStore.getSize());
//            } else {
//                logEJB.addLog(log);
//            }
//        }
//        } catch (Exception ex) {
//            System.out.println("*** Exception: " + ex);
//        }
      return "POST Test";
   }
}
