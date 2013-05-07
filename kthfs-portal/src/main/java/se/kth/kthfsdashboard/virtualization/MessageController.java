/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.virtualization;

/**
 *
 * @author Alberto Lorente Leal <albll@kth.se>
 */
import java.io.Serializable;
import java.util.ArrayList;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import javax.faces.bean.SessionScoped;
import javax.faces.context.FacesContext;

@ManagedBean
@SessionScoped
public class MessageController implements Serializable {
    
    private ArrayList<String> messageStatus = new ArrayList();
    private String lastMessage = "Preparing to submit operation";
    
    public void addMessage(String message) {
        messageStatus.add(message);
    }
    
    public void clearMessages() {
        messageStatus.clear();
        messageStatus.add("Preparing to submit operation");
    }
    
    public String showMessage() {
        if (messageStatus.isEmpty()) {
            return lastMessage;
        } else {
            String message = messageStatus.remove(0);
            lastMessage = message;
            return message;
        }
    }
    
    public void addErrorMessage(String exception) {
        statusMessage(null, FacesMessage.SEVERITY_WARN, "Warning", exception);
        
    }
    
    public void addSuccessMessage(String info) {
        statusMessage("success", FacesMessage.SEVERITY_INFO, "Success",info);
    }
    
    private void statusMessage(String key, FacesMessage.Severity severity, String message, String detail) {
        FacesMessage msg = new FacesMessage(severity, message, detail);
        FacesContext.getCurrentInstance().addMessage(key, msg);
    }
}
