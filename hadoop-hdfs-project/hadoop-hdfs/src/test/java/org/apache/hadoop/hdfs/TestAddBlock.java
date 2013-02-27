
package org.apache.hadoop.hdfs;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TestAddBlock {

    public static final Log LOG = LogFactory.getLog(TestAddBlock.class.getName());
    private final short DATANODES = 5;
    short replicas = 5;
    private int seed = 139;
    private int blockSize = 8192;
    Configuration conf;
    MiniDFSCluster cluster;
    DistributedFileSystem dfs;

    @Before
    public void initialize() throws IOException {
        conf = new HdfsConfiguration();
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODES).build();
    }

    @After
    public void close() {
        if (dfs != null) {
            IOUtils.closeStream(dfs);
        }
        cluster.shutdown();
        System.out.println("closed");
    }

    /**
     * Creates a file and writes a block using the returned output stream. It checks
     * if the block is written and checks if the number of located blocks and replicas 
     * are as expected.
     * @throws IOException 
     */
    @Test
    public void testAddBlock() throws IOException {

        cluster.waitActive();
        dfs = (DistributedFileSystem) cluster.getFileSystem();
        //DFSClient client = dfs.dfs;
        DFSClient client = dfs.getDefaultDFSClient();
        // create a new file.
        //
        Path file1 = new Path("/hooman.dat");

        FSDataOutputStream strm = createFile(dfs, file1, replicas);
        LOG.info("testAddBlock: "
                + "Created file hooman.dat with " + replicas + " replicas.");
        LocatedBlocks locations = client.getNamenode().getBlockLocations(
                file1.toString(), 0, Long.MAX_VALUE);
        LOG.info("testAddBlock: "
                + "The file has " + locations.locatedBlockCount() + " blocks.");

        // add one block to the file
        writeFile(strm, blockSize);
        strm.hflush();
        strm.close();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException ex) {
            Logger.getLogger(TestAddBlock.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        locations = client.getNamenode().getBlockLocations(
                file1.toString(), 0, Long.MAX_VALUE);

        //Check the number of blocks
        assert locations != null;
        assert locations.getLocatedBlocks().size() == 1;

        LocatedBlock location = locations.getLocatedBlocks().get(0);
        
        //check the number of replicas
        assertTrue("Location is null", location != null);
        assertTrue("wrong nubmer of blocks. blocks are "
                +location.getLocations().length
                +" expected were "+replicas, location.getLocations().length == replicas);

        
        //Check the validity of the replicas.
        for (DatanodeInfo dn : location.getLocations()) {
            assert dn != null;
        }
    }

    //
    // writes specified bytes to file.
    //
    private void writeFile(FSDataOutputStream stm, int size) throws IOException {
        byte[] buffer = AppendTestUtil.randomBytes(seed, size);
        stm.write(buffer, 0, size);
    }

    // creates a file but does not close it
    private FSDataOutputStream createFile(FileSystem fileSys, Path name, int repl)
            throws IOException {
        LOG.info("createFile: Created " + name + " with " + repl + " replica.");
        FSDataOutputStream stm = fileSys.create(name, true,
                fileSys.getConf().getInt("io.file.buffer.size", 4096),
                (short) repl, (long) blockSize);
        return stm;
    }
}
