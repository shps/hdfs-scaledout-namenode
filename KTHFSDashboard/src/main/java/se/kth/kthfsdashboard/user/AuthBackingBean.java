/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.user;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import javax.faces.context.FacesContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 *
 * @author Jim Dowling<jdowling@sics.se>
 */
@ManagedBean
 @RequestScoped 
//@SessionScoped
public class AuthBackingBean {

    private static Logger log = Logger.getLogger(AuthBackingBean.class.getName());
      private String username;  
      private String password;  
      
    private Username user; // The JPA entity.
    @EJB
    private UserFacade userService;

    public AuthBackingBean() {
    }

    private void addUser() {
        Username u = new Username();
        u.setEmail("basher");
        Group g = Group.ADMIN;
        List<Group> lg = new ArrayList<Group>();
        lg.add(g);
        u.setGroups(lg);
        u.setMobileNum("000");
        u.setName("Linda");
        u.setPassword("jim");
        u.setRegisteredOn(new Date());
        u.setUsername("lindass");
        u.setSalt("bl".getBytes());
        
        userService.persist(u);
        
    }
    
    public String login() {
        
        // addUser();
        FacesContext context = FacesContext.getCurrentInstance();
        HttpServletRequest request = (HttpServletRequest) context
                .getExternalContext().getRequest();

        try {
           
//           request.logout();
           request.login(username, password);
            user = userService.findByEmail(username);
        } catch (ServletException e) {
            context.addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Login failed!", null));
            return "login";
        }

        //you can fetch user from database for authenticated principal and do some action  
        Principal principal = request.getUserPrincipal();
        log.info("Logging IN Authenticated user: " + principal.getName());

        if (request.isUserInRole("ADMIN")) {
            return "/sauron/clusters.xml";
        } else {
            return "/sauron/clusters.xml";
        }
    }

    public String logout() {
        String result = "logout";

        FacesContext context = FacesContext.getCurrentInstance();
        if (context.getExternalContext().getRequest() == null) {
            return "/loginError.xml";  
        }
        HttpServletRequest request = (HttpServletRequest) context.getExternalContext().getRequest();
        Principal principal = request.getUserPrincipal();
        HttpSession s = request.getSession(false);
        log.log(Level.INFO, "Logging OUT Authenticated user: {0}", principal.getName());

        if (s != null) {
//            s.invalidate();
           try {  
               request.logout();  
           } catch (ServletException e) {  
                log.log(Level.SEVERE, "Failed to logout user!", e);  
                result = "/loginError.xml";  
           } catch (Throwable e)   {
                log.log(Level.SEVERE, "Throwable Exception when calling logout user! ", e.toString());  
           }
        }


        return result;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
        
}
