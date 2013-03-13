/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.virtualization.clusterparser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.RequestScoped;
import javax.faces.bean.SessionScoped;
import javax.faces.context.FacesContext;
import org.primefaces.event.FileUploadEvent;
import org.primefaces.model.UploadedFile;
import org.yaml.snakeyaml.Yaml;
import sun.awt.geom.Crossings;

/**
 *
 * @author Alberto Lorente Leal <albll@kth.se>
 */
@ManagedBean
@RequestScoped
public class ClusterController implements Serializable {

    private Cluster cluster;
    private boolean disableStart = true;
    private UploadedFile file;
    private Yaml yaml = new Yaml();

    /**
     * Creates a new instance of ClusterController
     */
    public ClusterController() {
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public UploadedFile getFile() {
        return file;
    }

    public void setFile(UploadedFile file) {
        this.file = file;
    }

    public void handleFileUpload(FileUploadEvent event) {
        file = event.getFile();
        parseYMLtoCluster();
        FacesMessage msg = new FacesMessage("Succesful", event.getFile().getFileName() + " is uploaded.");
        FacesContext.getCurrentInstance().addMessage(null, msg);
    }

    private void parseYMLtoCluster() {
        try {
            Object document = yaml.load(file.getInputstream());

            if (document != null && document instanceof Cluster) {
                disableStart = false;
                cluster = (Cluster) document;
            } else {
                disableStart = true;
                cluster = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }



    }

    public boolean clusterLoaded() {
        return disableStart;
    }
}
