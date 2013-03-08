package se.kth.kthfsdashboard.rest.resources;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.security.RolesAllowed;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.imageio.ImageIO;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import se.kth.kthfsdashboard.log.LogEJB;
import se.kth.kthfsdashboard.log.TempLogStore;
import se.kth.kthfsdashboard.util.CollectdTools;

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

   @GET
   @Path("graph")
   @Produces("image/png")
   public Response getGraph(
           @QueryParam("chart_type") String chartType,           
           @QueryParam("start") int start,
           @QueryParam("end") int end,
           @QueryParam("hostname") String hostname,
           @QueryParam("plugin") String plugin,
           @QueryParam("plugin_instance") String pluginInstance,
           @QueryParam("type") String type,
           @QueryParam("type_instance") String typeInstance,
           @QueryParam("ds") String ds 
           ) throws InterruptedException, IOException {

      CollectdTools ct = new CollectdTools();

      try {
         BufferedImage image;
         ByteArrayOutputStream baos = new ByteArrayOutputStream();
         InputStream g = ct.getGraphStream(chartType, hostname, plugin, pluginInstance, type, typeInstance, ds, start, end);
         image = ImageIO.read(g);
         ImageIO.write(image, "png", baos);
         byte[] imageData = baos.toByteArray();

         return Response.ok(new ByteArrayInputStream(imageData)).build();
      } catch (Exception e) {
         System.err.println("image == null!");
         return Response.status(Response.Status.NOT_FOUND).build();
      }
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
