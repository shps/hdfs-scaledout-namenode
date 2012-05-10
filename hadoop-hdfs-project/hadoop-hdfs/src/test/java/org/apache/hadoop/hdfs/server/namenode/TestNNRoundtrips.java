
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.metrics.HelperMetrics;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TestNNRoundtrips {
  
  public static final Log LOG = LogFactory.getLog(TestNNRoundtrips.class.getName());
  
  @BeforeClass
  public static void initDBConnector()
  {
    DBConnector.setConfiguration(new HdfsConfiguration());
  }
  
  @Test
  public void testCreateDirectory() throws IOException
  {
    Configuration conf = new HdfsConfiguration();
    
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).build();
    
    try
    {
      FileSystem fs = cluster.getWritingFileSystem();

      HelperMetrics.reset(); //set the metrics to zero

      Path path = new Path("/test1");
      fs.mkdirs(path);

      System.out.println(HelperMetrics.inodeMetrics.toString());
      
      Assert.assertTrue(true);
    }
    finally
    {
      cluster.shutdown();
    }
  }
  
  
}
