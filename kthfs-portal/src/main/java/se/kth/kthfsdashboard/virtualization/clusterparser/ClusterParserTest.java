/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package se.kth.kthfsdashboard.virtualization.clusterparser;

import bsh.This;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import junit.framework.TestCase;
import org.yaml.snakeyaml.Yaml;

/**
 *
 * @author Alberto Lorente Leal <albll@kth.se>
 */
public class ClusterParserTest extends TestCase {
    public static void main(String[] args) {
        Yaml yaml = new Yaml();
        final String dir = System.getProperty("user.dir");
        final String separator = System.getProperty("file.separator");


        try {
            Object document = yaml.load(new BufferedReader(new FileReader(new File(dir + separator + "ClusterDraft.yml"))));
            assertNotNull(document);
            assertTrue(document.getClass().toString(), document instanceof Cluster);
            Cluster cluster =(Cluster) document;
            System.out.println(cluster.getName());
            System.out.println(cluster.getKthfs());
            System.out.println(cluster.getYarn());
            System.out.println(cluster.getProvider().toString());
            System.out.println(cluster.getInstances().toString());
            System.out.println(cluster.getChefAttributes().toString());
            Map<String,String> map = cluster.getChefAttributes().get(0).getRoles().get(0).getAttributes();
            System.out.println(map.keySet());
            System.out.println(map.get("foo"));
            System.out.println(map.get("bar"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.out.println("File not found in the directory specified");
        }
    }
}
