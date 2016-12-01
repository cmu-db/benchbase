/******************************************************************************
 *  Copyright 2015 by OLTPBenchmark Project                                   *
 *                                                                            *
 *  Licensed under the Apache License, Version 2.0 (the "License");           *
 *  you may not use this file except in compliance with the License.          *
 *  You may obtain a copy of the License at                                   *
 *                                                                            *
 *    http://www.apache.org/licenses/LICENSE-2.0                              *
 *                                                                            *
 *  Unless required by applicable law or agreed to in writing, software       *
 *  distributed under the License is distributed on an "AS IS" BASIS,         *
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  *
 *  See the License for the specific language governing permissions and       *
 *  limitations under the License.                                            *
 ******************************************************************************/

package com.oltpbenchmark.benchmarks.linkbench;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;

import org.apache.log4j.Logger;

import com.oltpbenchmark.benchmarks.linkbench.distributions.*;
import com.oltpbenchmark.benchmarks.linkbench.distributions.RealDistribution.DistributionType;
import com.oltpbenchmark.benchmarks.linkbench.distributions.AccessDistributions.*;
import com.oltpbenchmark.benchmarks.linkbench.generators.*;
import com.oltpbenchmark.api.*;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.benchmarks.linkbench.pojo.*;
import com.oltpbenchmark.benchmarks.linkbench.procedures.*;
import com.oltpbenchmark.benchmarks.linkbench.utils.*;
import com.oltpbenchmark.types.*;
import com.oltpbenchmark.util.*;

public class LinkBenchWorker extends Worker<LinkBenchBenchmark> {

    private static final Logger LOG = Logger.getLogger(LinkBenchWorker.class);
    private Random rng;

    private Properties props;

    // Last node id accessed
    long lastNodeId;
    // Other informational counters
    long numfound = 0;
    long numnotfound = 0;
    long numHistoryQueries = 0;

    int nrequesters;
    int requesterID;
    long maxid1;
    long startid1;
    boolean singleAssoc = false;

    // Cumulative percentages
    double pc_addlink;
    double pc_deletelink;
    double pc_updatelink;
    double pc_countlink;
    double pc_getlink;
    double pc_getlinklist;
    double pc_addnode;
    double pc_deletenode;
    double pc_updatenode;
    double pc_getnode;

    // Chance of doing historical range query
    double p_historical_getlinklist;
    
    // Access distributions
    private AccessDistribution writeDist; // link writes
    private AccessDistribution writeDistUncorr; // to blend with link writes
    private double writeDistUncorrBlend; // Percentage to used writeDist2 for
    private AccessDistribution readDist; // link reads
    private AccessDistribution readDistUncorr; // to blend with link reads
    private double readDistUncorrBlend; // Percentage to used readDist2 for
    private AccessDistribution nodeReadDist; // node reads
    private AccessDistribution nodeUpdateDist; // node writes
    private AccessDistribution nodeDeleteDist; // node deletes

    // Control data generation settings
    private LogNormalDistribution linkDataSize;
    private DataGenerator linkAddDataGen;
    private DataGenerator linkUpDataGen;
    private LogNormalDistribution nodeDataSize;
    private DataGenerator nodeAddDataGen;
    private DataGenerator nodeUpDataGen;

    private ID2Chooser id2chooser;

    // Probability distribution for ids in multiget
    ProbabilityDistribution multigetDist;

    private static class HistoryKey {
        public final long id1;
        public final long link_type;
        public HistoryKey(long id1, long link_type) {
            super();
            this.id1 = id1;
            this.link_type = link_type;
        }

        public HistoryKey(Link l) {
            this(l.id1, l.link_type);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + (int) (id1 ^ (id1 >>> 32));
            result = prime * result + (int) (link_type ^ (link_type >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof HistoryKey))
                return false;
            HistoryKey other = (HistoryKey) obj;
            return id1 == other.id1 && link_type == other.link_type;
        }

    }
    // Cache of last link in lists where full list wasn't retrieved
    ArrayList<Link> listTailHistory;

    // Index of history to avoid duplicates
    HashMap<HistoryKey, Integer> listTailHistoryIndex;

    // Limit of cache size
    private int listTailHistoryLimit;

    public LinkBenchWorker(LinkBenchBenchmark benchmarkModule, int id, 
            Random masterRandom, Properties props, int nrequesters) {
        super(benchmarkModule, id);
        this.rng = masterRandom;

        this.props = props;
        this.requesterID =  id;
        this.nrequesters = nrequesters;
        maxid1 = ConfigUtil.getLong(props, LinkBenchConstants.MAX_ID);
        startid1 = ConfigUtil.getLong(props, LinkBenchConstants.MIN_ID);

        // math functions may cause problems for id1 < 1
        if (startid1 <= 0) {
            throw new LinkBenchConfigError("startid1 must be >= 1");
        }
        if (maxid1 <= startid1) {
            throw new LinkBenchConfigError("maxid1 must be > startid1");
        }

        // is this a single assoc test?
        if (startid1 + 1 == maxid1) {
            singleAssoc = true;
            LOG.info("Testing single row assoc read.");
        }

        initRequestProbabilities(this.props);
        initLinkDataGeneration(this.props);
        initLinkRequestDistributions(this.props, requesterID, nrequesters);
        if (pc_getnode > pc_getlinklist) {
            //            // Load stuff for node workload if needed
            //            if (nodeStore == null) {
            //                throw new IllegalArgumentException("nodeStore not provided but non-zero " +
            //                "probability of node operation");
            //            }
            initNodeDataGeneration(props);
            initNodeRequestDistributions(props);
        }
        listTailHistoryLimit = 2048; // Hardcoded limit for now
        listTailHistory = new ArrayList<Link>(listTailHistoryLimit);
        listTailHistoryIndex = new HashMap<HistoryKey, Integer>();
        p_historical_getlinklist = ConfigUtil.getDouble(this.props,
                            LinkBenchConstants.PR_GETLINKLIST_HISTORY, 0.0) / 100;
        lastNodeId = startid1;
    }

    @Override
    protected TransactionStatus executeWork(TransactionType nextTrans) throws UserAbortException, SQLException {
        Class<? extends Procedure> procClass = nextTrans.getProcedureClass();

        if (procClass.equals(AddNode.class)) {
            addNode();
        } else if (procClass.equals(GetNode.class)) {
            getNode();
        } else if (procClass.equals(DeleteNode.class)) {
            deleteNode();
        } else if (procClass.equals(UpdateNode.class)) {
            updateNode();
        } else if (procClass.equals(GetLink.class)) {
            getLink();
        } else if (procClass.equals(AddLink.class)) {
            addLink();
        } else if (procClass.equals(DeleteLink.class)) {
            deleteLink();
        } else if (procClass.equals(UpdateLink.class)) {
            updateLink();
        } else if (procClass.equals(CountLink.class)) {
            countLink();
        } else if (procClass.equals(GetLinkList.class)) {
            getLinkList();
        }
        return (TransactionStatus.SUCCESS);
    }

    private void addNode() throws SQLException {
        AddNode proc = this.getProcedure(AddNode.class);
        assert (proc != null);
        Node newNode = createAddNode();
        lastNodeId = proc.run(conn, newNode);
        if (LOG.isDebugEnabled()) {
            LOG.debug("lastNodeId= " +lastNodeId);
          }
    }
    private void getNode() throws SQLException {
        GetNode proc = this.getProcedure(GetNode.class);
        assert (proc != null);
        long idToFetch = chooseRequestID(DistributionType.NODE_READS,
                lastNodeId);
        Node fetched = proc.run(conn, LinkBenchConstants.DEFAULT_NODE_TYPE, idToFetch);
        lastNodeId = idToFetch;
    }
    private void deleteNode() throws SQLException {
        DeleteNode proc = this.getProcedure(DeleteNode.class);
        assert (proc != null);
        long idToDelete = chooseRequestID(DistributionType.NODE_DELETES,
                lastNodeId);
        proc.run(conn, LinkBenchConstants.DEFAULT_NODE_TYPE, idToDelete);
        lastNodeId = idToDelete;
    }
    private void updateNode() throws SQLException {
        UpdateNode proc = this.getProcedure(UpdateNode.class);
        assert (proc != null);
        // Choose an id that has previously been created (but might have
        // been since deleted
        long upId = chooseRequestID(DistributionType.NODE_UPDATES,
                lastNodeId);
        // Generate new data randomly
        Node newNode = createUpdateNode(upId);
        proc.run(conn,newNode);
        lastNodeId = upId;
    }
    private void getLink() throws SQLException {
        GetLink proc = this.getProcedure(GetLink.class);
        assert (proc != null);
        Link link = new Link();
        long id1 = chooseRequestID(DistributionType.LINK_READS, link.id1);
        long link_type = id2chooser.chooseRandomLinkType(rng);
        int nid2s = 1;
        if (multigetDist != null) {
            nid2s = (int)multigetDist.choose(rng);
        }
        long id2s[] = id2chooser.chooseMultipleForOp(rng, id1, link_type, nid2s,
                ID2Chooser.P_GET_EXIST);

        int found = getLink(id1, link_type, id2s);
        assert(found >= 0 && found <= nid2s);

        if (found > 0) {
            numfound += found;
        } else {
            numnotfound += nid2s - found;
        }
    }
    private void addLink() throws SQLException {
        AddLink proc = this.getProcedure(AddLink.class);
        assert (proc != null);
        // generate add request
        Link link =  new Link();
        link.id1 = chooseRequestID(DistributionType.LINK_WRITES, link.id1);
        link.link_type = id2chooser.chooseRandomLinkType(rng);
        link.id2 = id2chooser.chooseForOp(rng, link.id1, link.link_type,
                                                ID2Chooser.P_ADD_EXIST);
        link.visibility = LinkBenchConstants.VISIBILITY_DEFAULT;
        link.version = 0;
        link.time = System.currentTimeMillis();
        link.data = linkAddDataGen.fill(rng,
                                      new byte[(int)linkDataSize.choose(rng)]);
        // no inverses for now
        boolean alreadyExists = proc.run(conn, link, true);
        boolean added = !alreadyExists;
    }
    private void deleteLink() throws SQLException{
        DeleteLink proc = this.getProcedure(DeleteLink.class);
        assert (proc != null);
        Link link = new Link();
        long id1 = chooseRequestID(DistributionType.LINK_WRITES, link.id1);
        long link_type = id2chooser.chooseRandomLinkType(rng);
        long id2 = id2chooser.chooseForOp(rng, id1, link_type,
                                          ID2Chooser.P_DELETE_EXIST);
        proc.run(conn, id1, link_type, id2, true, // no inverse
            false);
    }
    private void updateLink() throws SQLException{
        //yes, updateLink uses addlLink procedure .. 
        AddLink proc = this.getProcedure(AddLink.class);
        assert (proc != null);
        Link link = new Link();
        link.id1 = chooseRequestID(DistributionType.LINK_WRITES, link.id1);
        link.link_type = id2chooser.chooseRandomLinkType(rng);
        // Update one of the existing links
        link.id2 = id2chooser.chooseForOp(rng, link.id1, link.link_type,
                                              ID2Chooser.P_UPDATE_EXIST);
        link.visibility = LinkBenchConstants.VISIBILITY_DEFAULT;
        link.version = 0;
        link.time = System.currentTimeMillis();
        link.data = linkUpDataGen.fill(rng,
                            new byte[(int)linkDataSize.choose(rng)]);
        // no inverses for now
        boolean found1 = proc.run(conn, link, true);
        boolean found = found1;
    }
    private void countLink() throws SQLException{
        //yes, updateLink uses addlLink procedure .. 
        CountLink proc = this.getProcedure(CountLink.class);
        assert (proc != null);
        Link link = new Link();
        long id1 = chooseRequestID(DistributionType.LINK_READS, link.id1);
        long link_type = id2chooser.chooseRandomLinkType(rng);
        long count = proc.run(conn, id1, link_type);
    }
    private void getLinkList() throws SQLException{
        //yes, updateLink uses addlLink procedure .. 
        GetLinkList proc = this.getProcedure(GetLinkList.class);
        assert (proc != null);
        Link link = new Link();
        Link links[];
        if (rng.nextDouble() < p_historical_getlinklist &&
                    !this.listTailHistory.isEmpty()) {
          links = getLinkListTail();
        } else {
          long id1 = chooseRequestID(DistributionType.LINK_READS, link.id1);
          long link_type = id2chooser.chooseRandomLinkType(rng);
          links = getLinkList(id1, link_type);
        }

        int count = ((links == null) ? 0 : links.length);
//        if (recordStats) {
//          stats.addStats(LinkBenchOp.RANGE_SIZE, count, false);
//        }
    }

    private void initRequestProbabilities(Properties props) {
        pc_addlink = ConfigUtil.getDouble(props, LinkBenchConstants.PR_ADD_LINK);
        pc_deletelink = pc_addlink + ConfigUtil.getDouble(props, LinkBenchConstants.PR_DELETE_LINK);
        pc_updatelink = pc_deletelink + ConfigUtil.getDouble(props, LinkBenchConstants.PR_UPDATE_LINK);
        pc_countlink = pc_updatelink + ConfigUtil.getDouble(props, LinkBenchConstants.PR_COUNT_LINKS);
        pc_getlink = pc_countlink + ConfigUtil.getDouble(props, LinkBenchConstants.PR_GET_LINK);
        pc_getlinklist = pc_getlink + ConfigUtil.getDouble(props, LinkBenchConstants.PR_GET_LINK_LIST);

        pc_addnode = pc_getlinklist + ConfigUtil.getDouble(props, LinkBenchConstants.PR_ADD_NODE, 0.0);
        pc_updatenode = pc_addnode + ConfigUtil.getDouble(props, LinkBenchConstants.PR_UPDATE_NODE, 0.0);
        pc_deletenode = pc_updatenode + ConfigUtil.getDouble(props, LinkBenchConstants.PR_DELETE_NODE, 0.0);
        pc_getnode = pc_deletenode + ConfigUtil.getDouble(props, LinkBenchConstants.PR_GET_NODE, 0.0);

        if (Math.abs(pc_getnode - 100.0) > 1e-5) {//compare real numbers
            throw new LinkBenchConfigError("Percentages of request types do not " +
                    "add to 100, only " + pc_getnode + "!");
        }
    }

    private void initLinkRequestDistributions(Properties props, int requesterID,
            int nrequesters) {
        writeDist = AccessDistributions.loadAccessDistribution(props,
                startid1, maxid1, DistributionType.LINK_WRITES);
        readDist = AccessDistributions.loadAccessDistribution(props,
                startid1, maxid1, DistributionType.LINK_READS);

        // Load uncorrelated distributions for blending if needed
        writeDistUncorr = null;
        if (props.containsKey(LinkBenchConstants.WRITE_UNCORR_BLEND)) {
            // Ratio of queries to use uncorrelated.  Convert from percentage
            writeDistUncorrBlend = ConfigUtil.getDouble(props,
                    LinkBenchConstants.WRITE_UNCORR_BLEND) / 100.0;
            if (writeDistUncorrBlend > 0.0) {
                writeDistUncorr = AccessDistributions.loadAccessDistribution(props,
                        startid1, maxid1, DistributionType.LINK_WRITES_UNCORR);
            }
        }

        readDistUncorr = null;
        if (props.containsKey(LinkBenchConstants.READ_UNCORR_BLEND)) {
            // Ratio of queries to use uncorrelated.  Convert from percentage
            readDistUncorrBlend = ConfigUtil.getDouble(props,
                    LinkBenchConstants.READ_UNCORR_BLEND) / 100.0;
            if (readDistUncorrBlend > 0.0) {
                readDistUncorr = AccessDistributions.loadAccessDistribution(props,
                        startid1, maxid1, DistributionType.LINK_READS_UNCORR);
            }
        }

        id2chooser = new ID2Chooser(props, startid1, maxid1,
                nrequesters, requesterID);

        // Distribution of #id2s per multiget
        String multigetDistClass = props.getProperty(LinkBenchConstants.LINK_MULTIGET_DIST);
        if (multigetDistClass != null && multigetDistClass.trim().length() != 0) {
            int multigetMin = ConfigUtil.getInt(props, LinkBenchConstants.LINK_MULTIGET_DIST_MIN);
            int multigetMax = ConfigUtil.getInt(props, LinkBenchConstants.LINK_MULTIGET_DIST_MAX);
            try {
                multigetDist = ClassUtil.newInstance(multigetDistClass,
                        ProbabilityDistribution.class);
                multigetDist.init(multigetMin, multigetMax, props,
                        LinkBenchConstants.LINK_MULTIGET_DIST_PREFIX);
            } catch (ClassNotFoundException e) {
                LOG.error(e);
                throw new LinkBenchConfigError("Class" + multigetDistClass +
                " could not be loaded as ProbabilityDistribution");
            }
        } else {
            multigetDist = null;
        }
    }

    private void initLinkDataGeneration(Properties props) {
        try {
            double medLinkDataSize = ConfigUtil.getDouble(props,
                    LinkBenchConstants.LINK_DATASIZE);
            linkDataSize = new LogNormalDistribution();
            linkDataSize.init(0, LinkBenchConstants.MAX_LINK_DATA, medLinkDataSize,
                    LinkBenchConstants.LINK_DATASIZE_SIGMA);
            linkAddDataGen = ClassUtil.newInstance(
                    ConfigUtil.getPropertyRequired(props, LinkBenchConstants.LINK_ADD_DATAGEN),
                    DataGenerator.class);
            linkAddDataGen.init(props, LinkBenchConstants.LINK_ADD_DATAGEN_PREFIX);

            linkUpDataGen = ClassUtil.newInstance(
                    ConfigUtil.getPropertyRequired(props, LinkBenchConstants.LINK_UP_DATAGEN),
                    DataGenerator.class);
            linkUpDataGen.init(props, LinkBenchConstants.LINK_UP_DATAGEN_PREFIX);
        } catch (ClassNotFoundException ex) {
            LOG.error(ex);
            throw new LinkBenchConfigError("Error loading data generator class: "
                    + ex.getMessage());
        }
    }

    private void initNodeDataGeneration(Properties props) {
        try {
            double medNodeDataSize = ConfigUtil.getDouble(props,
                    LinkBenchConstants.NODE_DATASIZE);
            nodeDataSize = new LogNormalDistribution();
            nodeDataSize.init(0, LinkBenchConstants.MAX_NODE_DATA, medNodeDataSize,
                    LinkBenchConstants.NODE_DATASIZE_SIGMA);

            String dataGenClass = ConfigUtil.getPropertyRequired(props,
                    LinkBenchConstants.NODE_ADD_DATAGEN);
            nodeAddDataGen = ClassUtil.newInstance(dataGenClass,
                    DataGenerator.class);
            nodeAddDataGen.init(props, LinkBenchConstants.NODE_ADD_DATAGEN_PREFIX);

            dataGenClass = ConfigUtil.getPropertyRequired(props,
                    LinkBenchConstants.NODE_UP_DATAGEN);
            nodeUpDataGen = ClassUtil.newInstance(dataGenClass,
                    DataGenerator.class);
            nodeUpDataGen.init(props, LinkBenchConstants.NODE_UP_DATAGEN_PREFIX);
        } catch (ClassNotFoundException ex) {
            LOG.error(ex);
            throw new LinkBenchConfigError("Error loading data generator class: "
                    + ex.getMessage());
        }
    }

    private void initNodeRequestDistributions(Properties props) {
        try {
            nodeReadDist  = AccessDistributions.loadAccessDistribution(props,
                    startid1, maxid1, DistributionType.NODE_READS);
        } catch (LinkBenchConfigError e) {
            // Not defined
            LOG.info("Node access distribution not configured: " +
                    e.getMessage());
            throw new LinkBenchConfigError("Node read distribution not " +
            "configured but node read operations have non-zero probability");
        }

        try {
            nodeUpdateDist  = AccessDistributions.loadAccessDistribution(props,
                    startid1, maxid1, DistributionType.NODE_UPDATES);
        } catch (LinkBenchConfigError e) {
            // Not defined
            LOG.info("Node access distribution not configured: " +
                    e.getMessage());
            throw new LinkBenchConfigError("Node write distribution not " +
            "configured but node write operations have non-zero probability");
        }

        try {
            nodeDeleteDist = AccessDistributions.loadAccessDistribution(props,
                    startid1, maxid1, DistributionType.NODE_DELETES);
        } catch (LinkBenchConfigError e) {
            // Not defined
            LOG.info("Node delete distribution not configured: " +
                    e.getMessage());
            throw new LinkBenchConfigError("Node delete distribution not " +
            "configured but node write operations have non-zero probability");
        }
    }

    /**
     * Create a new node for adding to database
     * @return
     */
    private Node createAddNode() {
        byte data[] = nodeAddDataGen.fill(rng, new byte[(int)nodeDataSize.choose(rng)]);
        return new Node(-1, LinkBenchConstants.DEFAULT_NODE_TYPE, 1,
                (int)(System.currentTimeMillis()/1000), data);
    }

    // gets id1 for the request based on desired distribution
    private long chooseRequestID(DistributionType type, long previousId1) {
        AccessDistribution dist;
        switch (type) {
            case LINK_READS:
                // Blend between distributions if needed
                if (readDistUncorr == null || rng.nextDouble() >= readDistUncorrBlend) {
                    dist = readDist;
                } else {
                    dist = readDistUncorr;
                }
                break;
            case LINK_WRITES:
                // Blend between distributions if needed
                if (writeDistUncorr == null || rng.nextDouble() >= writeDistUncorrBlend) {
                    dist = writeDist;
                } else {
                    dist = writeDistUncorr;
                }
                break;
            case LINK_WRITES_UNCORR:
                dist = writeDistUncorr;
                break;
            case NODE_READS:
                dist = nodeReadDist;
                break;
            case NODE_UPDATES:
                dist = nodeUpdateDist;
                break;
            case NODE_DELETES:
                dist = nodeDeleteDist;
                break;
            default:
                throw new RuntimeException("Unknown value for type: " + type);
        }
        long newid1 = dist.nextID(rng, previousId1);
        // Distribution responsible for generating number in range
        assert((newid1 >= startid1) && (newid1 < maxid1));
        if (LOG.isDebugEnabled()) {
            LOG.trace("id1 generated = " + newid1 +
                    " for access distribution: " + dist.getClass().getName() + ": " +
                    dist.toString());
        }

        if (dist.getShuffler() != null) {
            // Shuffle to go from position in space ranked from most to least accessed,
            // to the real id space
            newid1 = startid1 + dist.getShuffler().permute(newid1 - startid1);
        }
        return newid1;
    }

    /**
     * Create new node for updating in database
     */
    private Node createUpdateNode(long id) {
        byte data[] = nodeUpDataGen.fill(rng, new byte[(int)nodeDataSize.choose(rng)]);
        return new Node(id, LinkBenchConstants.DEFAULT_NODE_TYPE, 2,
                (int)(System.currentTimeMillis()/1000), data);
    }
    int getLink(long id1, long link_type, long id2s[]) throws SQLException {
        Link links[] = multigetLinks(id1, link_type, id2s);
        return links == null ? 0 : links.length;
    }
    Link[] multigetLinks(long id1, long link_type, long id2s[]) throws SQLException {
        GetLink proc= this.getProcedure(GetLink.class);
        Link links[] = proc.run(conn, id1, link_type, id2s);
        if (LOG.isDebugEnabled()) {
            LOG.trace("getLinkList(id1=" + id1 + ", link_type="  + link_type
                    + ") => count=" + (links == null ? 0 : links.length));
        }
        // If there were more links than limit, record
        if (links != null && links.length >= LinkBenchConstants.DEFAULT_LIMIT) {
            Link lastLink = links[links.length-1];
            if (LOG.isDebugEnabled()) {
                LOG.trace("Maybe more history for (" + id1 +"," +
                        link_type + " older than " + lastLink.time);
            }

            addTailCacheEntry(lastLink);
        }
        return links;
    }
    Link[] getLinkList(long id1, long link_type) throws SQLException {
        GetLinkList proc= this.getProcedure(GetLinkList.class);
        Link links[] = proc.run(conn, id1, link_type);
        if (LOG.isDebugEnabled()) {
           LOG.trace("getLinkList(id1=" + id1 + ", link_type="  + link_type
                         + ") => count=" + (links == null ? 0 : links.length));
        }
        // If there were more links than limit, record
        if (links != null && links.length >= LinkBenchConstants.DEFAULT_LIMIT) {
          Link lastLink = links[links.length-1];
          if (LOG.isDebugEnabled()) {
            LOG.trace("Maybe more history for (" + id1 +"," +
                          link_type + " older than " + lastLink.time);
          }

          addTailCacheEntry(lastLink);
        }
        return links;
      }
    Link[] getLinkListTail() throws SQLException {
        GetLinkList proc = this.getProcedure(GetLinkList.class);
        assert(!listTailHistoryIndex.isEmpty());
        assert(!listTailHistory.isEmpty());
        int choice = rng.nextInt(listTailHistory.size());
        Link prevLast = listTailHistory.get(choice);

        // Get links past the oldest last retrieved
        Link links[] = proc.run(conn, prevLast.id1,
            prevLast.link_type, 0, prevLast.time, 1, LinkBenchConstants.DEFAULT_LIMIT);

        if (LOG.isDebugEnabled()) {
          LOG.trace("getLinkListTail(id1=" + prevLast.id1 + ", link_type="
                    + prevLast.link_type + ", max_time=" + prevLast.time
                    + " => count=" + (links == null ? 0 : links.length));
       }
        if (LOG.isDebugEnabled()) {
            LOG.trace("Historical range query for (" + prevLast.id1 +"," +
                        prevLast.link_type + " older than " + prevLast.time +
                        ": " + (links == null ? 0 : links.length) + " results");
        }

        if (links != null && links.length == LinkBenchConstants.DEFAULT_LIMIT) {
          // There might be yet more history
          Link last = links[links.length-1];
          if (LOG.isDebugEnabled()) {
              LOG.trace("might be yet more history for (" + last.id1 +"," +
                          last.link_type + " older than " + last.time);
          }
          // Update in place
          listTailHistory.set(choice, last.clone());
        } else {
          // No more history after this, remove from cache
          removeTailCacheEntry(choice, null);
        }
        numHistoryQueries++;
        return links;
      }
    /**
     * Add a new link to the history cache, unless already present
     * @param lastLink the last (i.e. lowest timestamp) link retrieved
     */
    private void addTailCacheEntry(Link lastLink) {
        HistoryKey key = new HistoryKey(lastLink);
        if (listTailHistoryIndex.containsKey(key)) {
            // Already present
            return;
        }

        if (listTailHistory.size() < listTailHistoryLimit) {
            listTailHistory.add(lastLink.clone());
            listTailHistoryIndex.put(key, listTailHistory.size() - 1);
        } else {
            // Need to evict entry
            int choice = rng.nextInt(listTailHistory.size());
            removeTailCacheEntry(choice, lastLink.clone());
        }
    }
    /**
     * Remove or replace entry in listTailHistory and update index
     * @param pos index of entry in listTailHistory
     * @param repl replace with this if not null
     */
    private void removeTailCacheEntry(int pos, Link repl) {
        Link entry = listTailHistory.get(pos);
        if (pos == listTailHistory.size() - 1) {
            // removing from last position, don't need to fill gap
            listTailHistoryIndex.remove(new HistoryKey(entry));
            int lastIx = listTailHistory.size() - 1;
            if (repl == null) {
                listTailHistory.remove(lastIx);
            } else {
                listTailHistory.set(lastIx, repl);
                listTailHistoryIndex.put(new HistoryKey(repl), lastIx);
            }
        } else {
            if (repl == null) {
                // Replace with last entry in cache to fill gap
                repl = listTailHistory.get(listTailHistory.size() - 1);
                listTailHistory.remove(listTailHistory.size() - 1);
            }
            listTailHistory.set(pos, repl);
            listTailHistoryIndex.put(new HistoryKey(repl), pos);
        }
    }
}
