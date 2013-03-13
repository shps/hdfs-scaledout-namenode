/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.virtualization.clusterparser;


/**
 *
 * @author Alberto Lorente Leal <albll@kth.se>
 */
public class ChefAttributes {
    private String role;
    private String chefJson;

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getChefJson() {
        return chefJson;
    }

    public void setChefJson(String chefJson) {
        this.chefJson = chefJson;
    }

    @Override
    public String toString() {
        return "ChefAttributes{" + "role=" + role + ", chefJson=" + chefJson + '}';
    }
       
    
}
