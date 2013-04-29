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
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;


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
    @ManyToOne
    @JoinColumn(name="CLUSTER_ID")
    private Cluster cluster;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getServicerole() {
        return servicerole;
    }

    public void setServicerole(String servicerole) {
        this.servicerole = servicerole;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }
    
    

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
