package se.kth.kthfsdashboard.command;

import java.io.Serializable;
import java.util.HashMap;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ApplicationScoped;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;
import javax.faces.event.ActionEvent;
import org.primefaces.context.RequestContext;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
@ManagedBean
@ViewScoped
//@ApplicationScoped
public class ExecuteCommandController implements Serializable {

   private static HashMap<String, Integer> progress = new HashMap<String, Integer>();

   public ExecuteCommandController() {
   }

   public void onComplete() {
      FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_INFO, "Progress Completed", "Progress Completed"));
   }

   public void cancel() {
      progress = null;
   }

   public void doCommandTest(ActionEvent actionEvent) throws InterruptedException {




      progress.put("a1", 35);
      progress.put("a2", 20);

      Thread.sleep(2000);

      progress.put("a1", 100);
      progress.put("a2", 70);      
      
      Thread.sleep(4000);

      progress.put("a2", 100);      
      System.err.println("####################" );

   }

   public Integer progressValue(String id) {
      RequestContext context = RequestContext.getCurrentInstance();
//      context.execute("prog.start()");

      System.err.println("@@@ a=" + progress.get(id));

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
