package se.kth.kthfsdashboard.command;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;
import javax.faces.event.ActionEvent;
import org.primefaces.context.RequestContext;
import se.kth.kthfsdashboard.service.Service;
import se.kth.kthfsdashboard.struct.InstanceFullInfo;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@ViewScoped
//@ApplicationScoped
public class ExecuteCommandController implements Serializable {

   private static HashMap<InstanceFullInfo, Integer> progress = new HashMap<InstanceFullInfo, Integer>();
   private static List<InstanceFullInfo> services = new ArrayList<InstanceFullInfo>();

   public ExecuteCommandController() {

      InstanceFullInfo i1 = new InstanceFullInfo("kthfs1", "namenode", "namenode", "cloud1.sics.se", 0, "/default", Service.Status.Started, null);
      InstanceFullInfo i2 = new InstanceFullInfo("kthfs1", "datanode", "datanode", "cloud2.sics.se", 0, "/default", Service.Status.Started, null);

      services.clear();
      services.add(i1);
      services.add(i2);

   }

   public List<InstanceFullInfo> getServices() {
      return services;
   }

   public void onComplete() {
      FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_INFO, "Progress Completed", "Progress Completed"));
   }

   public void cancel() {
      progress = null;
   }

   public void doCommandTest(ActionEvent actionEvent) throws InterruptedException {
      
      
  RequestContext requestContext = RequestContext.getCurrentInstance();  
  requestContext.execute("alert('hello')");



      Command c = new Command("Start", "cloud1.sics.se", "namenode", "namenode", "kthfs1");

      for (InstanceFullInfo i : services) {

         progress.put(i, 0);
      }

      Thread.sleep(1000);


      int min = 0;

      while (min < 100) {
         int d = 30;
         min = 100;
         for (InstanceFullInfo i : services) {


            int val = progress.get(i) + d;
            if (val > 100) {
               val = 100;
            }
            d += 10;
            progress.put(i, val);
            
           if (val < min) {
              min = val;
           }
            
            System.err.println("HERE, val=" + val);
         }
         Thread.sleep(2000);
      }

      System.err.println("####################");

   }

   public Integer progressValue(InstanceFullInfo id) {
      RequestContext context = RequestContext.getCurrentInstance();
//      context.execute("prog.start()");

      System.err.println("@@@ prog=" + progress.get(id));

      Integer value = progress.get(id);

      if (value == null) {
         value = 0;
      }

      if (value == 100) {
//         context.execute("prog.stop()");
         progress.remove(id);
         System.err.println("@@@ STOP");
      }

      return value;
   }
}
