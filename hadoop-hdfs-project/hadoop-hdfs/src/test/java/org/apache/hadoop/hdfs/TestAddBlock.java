/*
 * Copyright 2012 Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs;

import java.io.IOException;
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

/**
 *
 * @author Hooman <hooman@sics.se>
 */
public class TestAddBlock {

    public static final Log LOG = LogFactory.getLog(TestAddBlock.class.getName());
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
    }

    @After
    public void close() {
        if (dfs != null) {
            IOUtils.closeStream(dfs);
        }
        cluster.shutdown();
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
        dfs = (DistributedFileSystem) cluster.getWritingFileSystem();
        DFSClient client = dfs.dfs;
        // create a new file.
        //
        Path file1 = new Path("/hooman.dat");
        short replicas = 3;
        FSDataOutputStream strm = createFile(dfs, file1, 3);
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

        locations = client.getNamenode().getBlockLocations(
                file1.toString(), 0, Long.MAX_VALUE);

        //Check the number of blocks
        assert locations != null;
        assert locations.getLocatedBlocks().size() == 1;

        LocatedBlock location = locations.getLocatedBlocks().get(0);
        
        //check the number of replicas
        assert location != null;
        assert location.getLocations().length == replicas;

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
