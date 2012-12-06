package se.kth.kthfsdashboard.util;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.stamfest.rrd.CommandResult;
import net.stamfest.rrd.RRDp;

/**
 *
 * @author Hamidreza Afzali <afzali@kth.se>
 */
public class CollectdTools {

    private static final String COLLECTD_PATH = "/var/lib/collectd/rrd/";
    private static final String RRD_EXT = ".rrd";
    private static final String COLLECTD_LINK = "data/";
    //    public static final String COLLECTD_LIB = "/var/lib/collectd/rrd/";
    Parser parser = new Parser();

    public CollectdTools() {
    }

    public Set<String> pluginInstances(String hostName, String plugin) {

        final String p = plugin;
        Set<String> instances = new TreeSet<String>();
        File dir = new File(COLLECTD_PATH + hostName);

        File[] files = dir.listFiles(new FilenameFilter() {

            @Override
            public boolean accept(File dir, String name) {
                return name.startsWith(p.toString());
            }
        });

        if (files == null) {
            return instances;
        }
        for (File file : files) {
            instances.add(file.getName().split("-")[1]);
        }

        return instances;
    }

    public int pluginInstancesCount(String plugin, String hostName) {

        return pluginInstances(plugin, hostName).size();
    }

    public Set<String> typeInstances(String hostName, String plugin) {

        SortedSet<String> instances = new TreeSet<String>();
        File dir = new File(COLLECTD_PATH + hostName + "/" + plugin);

        File[] files = dir.listFiles();

        if (files == null) {
            return instances;
        }
        for (File file : files) {
            instances.add(file.getName().split(RRD_EXT)[0].split("-")[1]);
        }

        return instances;
    }

    public double[] getLastLoad(String hostname) {

        DecimalFormat format = new DecimalFormat("#.##");
        String loads[] = readLastRrdValue(hostname, "load", "", "load", "").split(":")[1].trim().toUpperCase().split(" ");
        double load[] = new double[3];
        try {
            load[0] = format.parse(loads[0]).doubleValue();
            load[1] = format.parse(loads[1]).doubleValue();
            load[2] = format.parse(loads[2]).doubleValue();
        } catch (ParseException ex) {
            Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        return load;
    }

    public String getTest(String hostname, String type) {
        return readLastRrdValue(hostname, "memory", "", "memory", type);
    }

    public Long getLatestMemoryStatus(String hostname, String type) {

        String res1 = readLastRrdValue(hostname, "memory", "", "memory", type);
        String result;
        if (res1.lastIndexOf(":") < 1) { // ERROR
            Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, "RRD: " + res1);
            return -1l;
        } else {
            result = res1.split(":")[1].trim().toUpperCase();
        }

        try {
            return parser.parseLong(result);
        } catch (ParseException ex) {

            System.err.println(ex + "- result='" + result + "'");
            Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, ex);
        }
        return null;
    }

    private String readLastRrdValue(String hostname, String plugin, String pluginInstance, String type, String typeInstance) {
        try {

            String pluginURI = pluginInstance.isEmpty() ? plugin : plugin + "-" + pluginInstance;
            String typeURI = typeInstance.isEmpty() ? type : type + "-" + typeInstance;
            RRDp rrd = new RRDp(COLLECTD_LINK + hostname + "/" + pluginURI + "/", "5555");

            //get latest recoded time
            CommandResult result = rrd.command(new String[]{"last", typeURI + RRD_EXT, "MAX"});
            String t = Long.toString(((long) Math.floor((Long.parseLong(result.output.trim()) / 10))) * 10 - 10);

            result = rrd.command(new String[]{"fetch", typeURI + RRD_EXT, "MIN", "-s", t, "-e", t});

            if (!result.ok) {
                System.err.println("ERROR in collectdTools: " + result.error);
                return result.error;
//                return "ERROR";
            } else {
                return result.output.split("\\r?\\n")[2];
            }
        } catch (Exception ex) {
            Logger.getLogger(CollectdTools.class.getName()).log(Level.SEVERE, null, ex);

            return "ERROR";
        }
    }
}
