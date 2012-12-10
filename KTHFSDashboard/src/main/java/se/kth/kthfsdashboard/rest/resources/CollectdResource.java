package se.kth.kthfsdashboard.rest.resources;

import java.util.Date;
import javax.annotation.security.PermitAll;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.codehaus.jettison.json.JSONObject;
import se.kth.kthfsdashboard.alert.Alert;
import se.kth.kthfsdashboard.alert.AlertEJB;
import se.kth.kthfsdashboard.host.Host;
import se.kth.kthfsdashboard.log.LogEJB;
import se.kth.kthfsdashboard.log.TempLogStore;

/**
 * :
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@Path("/collectd")
@Stateless
public class CollectdResource {

    @EJB
    private LogEJB logEJB;
    @EJB
    private AlertEJB alertEJB;
    
    TempLogStore tempLogStore = new TempLogStore();

    @GET
    @PermitAll
    @Produces(MediaType.TEXT_PLAIN)
    public String getLog() {

        System.out.println("GET");

        return "GET: collectd";
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public String postLog(@Context HttpServletRequest Req, String jsonArrayStrig)  {

//        try {
//
//        JSONArray jsonArray = new JSONArray(jsonArrayStrig);
//
//        for (int i = 0; i < jsonArray.length(); i++) {
//
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
//
//        }
//        } catch (Exception ex) {
//            System.out.println("***************** Exception ******************" + ex);
//        }
//
//        System.out.println("POST");



        return "POST Test";
    }
    
    
    @Path("/alert")
    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response keepAlive(@Context HttpServletRequest req, String jsonStrig) {
       
//       TODO: Alerts are stored in the database. We must define reactions later (Email, SMS, ....).
        try {
            JSONObject json = new JSONObject(jsonStrig);
            
//            System.err.println(json);
            
            Alert alert = new Alert();
            alert.setAlertTime(new Date());
            
            alert.setAgentTime(json.getLong("Time"));
            alert.setMessage(json.getString("Message"));
            alert.setHostname(json.getString("Host"));
            alert.setSeverity(Alert.Severity.valueOf(json.getString("Severity")));
            
            alert.setPlugin(json.getString("Plugin"));
            if (json.has("PluginInstance")){
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
