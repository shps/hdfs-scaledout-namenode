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

}
