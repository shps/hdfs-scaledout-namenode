/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.virtualization.clusterparser;

import java.io.Serializable;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;


/**
 *
 * @author Alberto Lorente Leal <albll@kth.se>
 */
@Entity
public class ChefAttributes implements Serializable{
    @Id
    @GeneratedValue(strategy= GenerationType.AUTO)
    private Long id;
    private String servicerole;
    private String chefJson;

    public String getRole() {
        return servicerole;
    }

    public void setRole(String role) {
        this.servicerole = role;
    }

    public String getChefJson() {
        return chefJson;
    }

    public void setChefJson(String chefJson) {
        this.chefJson = chefJson;
    }

    @Override
    public String toString() {
        return "ChefAttributes{" + "role=" + servicerole + ", chefJson=" + chefJson + '}';
    }
       
    
}
