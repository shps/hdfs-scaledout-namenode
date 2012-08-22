package org.apache.hadoop.hdfs.server.namenode.persistance;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.AppendTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 * @author kamal hakimzadeh <mahh@kth.se>
 */
public class GetAdditionalBlockLockAcquisitionTest {

    public static final Log LOG = LogFactory.getLog(GetAdditionalBlockLockAcquisitionTest.class.getName());
    private final short DATANODES = 5;
    private int seed = 139;
    private int blockSize = 8192;
    Configuration conf;
    MiniDFSCluster cluster;
    DistributedFileSystem dfs;

    @Before
    public void initialize() throws IOException {
        conf = new HdfsConfiguration();
        cluster = new MiniDFSCluster.Builder(conf).numDataNodes(DATANODES).build();
        cluster.waitActive();
    }

    @After
    public void close() {
        if (dfs != null) {
            IOUtils.closeStream(dfs);
        }
        cluster.shutdown();
    }

    /**
     * writing 3 blocks into a file with replication factor 3
     * @throws IOException 
     */
    @Ignore
    @Test
    public void testScenario1() throws IOException {
        dfs = (DistributedFileSystem) cluster.getFileSystem();
        // create a new file.
        //
        Path file1 = new Path("/ed1/ed2/ed3/f");
        FSDataOutputStream strm = createFile(dfs, file1, 3);

        writeFile(strm, blockSize * 3);
        strm.hflush();
        strm.close();

    }
    
    /**
     *  append 2 blocks right after 2 existing blocks
     */
    @Test
    public void testScenario2() throws IOException {
        dfs = (DistributedFileSystem) cluster.getFileSystem();
        // create a new file.
        //
        Path file1 = new Path("/ed1/ed2/ed3/f");
        FSDataOutputStream strm = createFile(dfs, file1, 3);

        writeFile(strm, blockSize * 2);
        strm.hflush();
        strm.close();
        
        strm = dfs.append(file1);
        writeFile(strm, blockSize*2);
        strm.flush();
        strm.close();
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
