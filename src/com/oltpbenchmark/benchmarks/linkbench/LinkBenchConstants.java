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

public abstract class LinkBenchConstants {

    public static final long MAX_NODE_DATA = 1024 * 1024;
    public static final int START_ID = 1;
    public static final int configCommitCount = 100;

    // visibility
    public static final byte VISIBILITY_HIDDEN = 0;
    public static final byte VISIBILITY_DEFAULT = 1;
    
    public static final long DEFAULT_LINK_TYPE = 123456789;
    public static final long MAX_ID2 = Long.MAX_VALUE;
    public static final int DEFAULT_NODE_TYPE = 2048;
    public static final long MAX_LINK_DATA = 255;

    public static final int DEFAULT_LIMIT = 10000;
    
    public static final String DEBUGLEVEL = "debuglevel";

    /* Control store implementations used */
    public static final String LINKSTORE_CLASS = "linkstore";
    public static final String NODESTORE_CLASS = "nodestore";

    /* Schema and tables used */
    public static final String DBID = "dbid";
    public static final String LINK_TABLE = "linktable";
    public static final String COUNT_TABLE = "counttable";
    public static final String NODE_TABLE = "nodetable";

    /* Control graph structure */
    public static final String LOAD_RANDOM_SEED = "load_random_seed";
    public static final String MIN_ID = "startid1";
    public static final String MAX_ID = "maxid1";
    public static final String GENERATE_NODES = "generate_nodes";
    public static final String RANDOM_ID2_MAX = "randomid2max";
    public static final String NLINKS_PREFIX = "nlinks_";
    public static final String NLINKS_FUNC = "nlinks_func";
    public static final String NLINKS_CONFIG = "nlinks_config";
    public static final String NLINKS_DEFAULT = "nlinks_default";
    public static final String LINK_TYPE_COUNT ="link_type_count";

    /* Data generation */
    public static final String LINK_DATASIZE = "link_datasize";
    public static final String NODE_DATASIZE = "node_datasize";
    public static final String UNIFORM_GEN_STARTBYTE = "startbyte";
    public static final String UNIFORM_GEN_ENDBYTE = "endbyte";
    public static final String MOTIF_GEN_UNIQUENESS = "uniqueness";
    public static final String MOTIF_GEN_LENGTH = "motif_length";
    public static final String LINK_ADD_DATAGEN = "link_add_datagen";
    public static final String LINK_ADD_DATAGEN_PREFIX = "link_add_datagen_";
    public static final String LINK_UP_DATAGEN = "link_up_datagen";
    public static final String LINK_UP_DATAGEN_PREFIX = "link_up_datagen_";
    public static final String NODE_ADD_DATAGEN = "node_add_datagen";
    public static final String NODE_ADD_DATAGEN_PREFIX = "node_add_datagen_";
    public static final String NODE_UP_DATAGEN = "node_up_datagen";
    public static final String NODE_UP_DATAGEN_PREFIX = "node_up_datagen_";
    // Sigma values control variance of data size log normal distribution
    public static final double LINK_DATASIZE_SIGMA = 1.0;
    public static final double NODE_DATASIZE_SIGMA = 1.0;

    /* Loading performance tuning */
    public static final String NUM_LOADERS = "loaders";
    public static final String LOADER_CHUNK_SIZE = "loader_chunk_size";

    /* Request workload */
    public static final String NUM_REQUESTERS = "requesters";
    public static final String REQUEST_RANDOM_SEED = "request_random_seed";

    // Distribution of accesses to IDs
    public static final String READ_CONFIG_PREFIX = "read_";
    public static final String WRITE_CONFIG_PREFIX = "write_";
    public static final String NODE_READ_CONFIG_PREFIX = "node_read_";
    public static final String NODE_UPDATE_CONFIG_PREFIX = "node_update_";
    public static final String NODE_DELETE_CONFIG_PREFIX = "node_delete_";
    public static final String ACCESS_FUNCTION_SUFFIX = "function";
    public static final String ACCESS_CONFIG_SUFFIX = "config";
    public static final String READ_FUNCTION = "read_function";
    public static final String READ_CONFIG = "read_config";
    public static final String WRITE_FUNCTION = "write_function";
    public static final String WRITE_CONFIG = "write_config";
    public static final String READ_UNCORR_CONFIG_PREFIX = "read_uncorr_";
    public static final String WRITE_UNCORR_CONFIG_PREFIX = "read_uncorr_";
    public static final String READ_UNCORR_FUNCTION = READ_UNCORR_CONFIG_PREFIX
                                                      + ACCESS_FUNCTION_SUFFIX;
    public static final String WRITE_UNCORR_FUNCTION = WRITE_UNCORR_CONFIG_PREFIX
                                                      + ACCESS_FUNCTION_SUFFIX;
    public static final String BLEND_SUFFIX = "blend";
    public static final String READ_UNCORR_BLEND =  READ_UNCORR_CONFIG_PREFIX
                                                      + BLEND_SUFFIX;
    public static final String WRITE_UNCORR_BLEND = WRITE_UNCORR_CONFIG_PREFIX
                                                      + BLEND_SUFFIX;

    // Probability of different operations
    public static final String PR_ADD_LINK = "addlink";
    public static final String PR_DELETE_LINK = "deletelink";
    public static final String PR_UPDATE_LINK = "updatelink";
    public static final String PR_COUNT_LINKS = "countlink";
    public static final String PR_GET_LINK = "getlink";
    public static final String PR_GET_LINK_LIST = "getlinklist";
    public static final String PR_ADD_NODE = "addnode";
    public static final String PR_UPDATE_NODE = "updatenode";
    public static final String PR_DELETE_NODE = "deletenode";
    public static final String PR_GET_NODE = "getnode";
    public static final String PR_GETLINKLIST_HISTORY = "getlinklist_history";
    public static final String WARMUP_TIME = "warmup_time";
    public static final String MAX_TIME = "maxtime";
    public static final String REQUEST_RATE = "requestrate";
    public static final String NUM_REQUESTS = "requests";
    public static final String MAX_FAILED_REQUESTS = "max_failed_requests";
    public static final String ID2GEN_CONFIG = "id2gen_config";
    public static final String LINK_MULTIGET_DIST = "link_multiget_dist";
    public static final String LINK_MULTIGET_DIST_MIN = "link_multiget_dist_min";
    public static final String LINK_MULTIGET_DIST_MAX = "link_multiget_dist_max";
    public static final String LINK_MULTIGET_DIST_PREFIX = "link_multiget_dist_";

    /* Probability distribution parameters */
    public static final String PROB_MEAN = "mean";

    /* Statistics collection and reporting */
    public static final String MAX_STAT_SAMPLES = "maxsamples";
    public static final String DISPLAY_FREQ = "displayfreq";
    public static final String MAPRED_REPORT_PROGRESS = "reportprogress";
    public static final String PROGRESS_FREQ = "progressfreq";

    /* Reporting for progress indicators */
    public static String REQ_PROG_INTERVAL = "req_progress_interval";
    public static String LOAD_PROG_INTERVAL = "load_progress_interval";

    /* MapReduce specific configuration */
    public static final String TEMPDIR = "tempdir";
    public static final String LOAD_DATA = "loaddata";
    public static final String MAPRED_USE_INPUT_FILES = "useinputfiles";

    /* External data */
    public static final String DISTRIBUTION_DATA_FILE = "data_file";
    public static final String WORKLOAD_CONFIG_FILE = "workload_file";

}
