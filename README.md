# OLTP-Bench: Part Deux

Forked from https://github.com/oltpbenchmark/oltpbench with a focus on cleanup and modernization.  Given the volume and scope of these changes, I have elected not to submit pull requests to the original project as it is unlikely they would or could be accepted.

See also: [OLTP-Bench: An extensible testbed for benchmarking relational databases](http://www.vldb.org/pvldb/vol7/p277-difallah.pdf) D. E. Difallah, A. Pavlo, C. Curino, and P. Cudre-Mauroux. In VLDB 2014.

## Modifications to Original
This fork contains a number of significant **structural** modifications to the original project.  This was done in an effort to cleanup and modernize the code base, not to alter the spirit or function of the project.  To this end, I did my best to leave the actual benchmark code **functionally** unchanged while improving where possible.  My modifications are summarized below:

* Moved from Ant to Maven
    * Reorganized project to fit Maven structure
    * Removed static `lib` directory and dependencies
    * Updated required dependencies and removed unused or unwanted dependencies
    * Moved all non `.java` files to standard Maven `resources` directory
* Improved packaging and versioning
    * Moved to Calendar Versioning (https://calver.org/)
    * Project is now distributed as a `.tgz` with executable `.jar`
    * All code updated to read `resources` from inside `.jar` instead of directory
* Built with and for Java 1.8
* Moved from direct dependence on Log4J to SLF4J
* Reorganized and renamed many files (mostly `resources`) for clarity and consistency
* Applied countless fixes based on "Static Analysis"
    * JDK migrations (boxing, un-boxing, etc.)
    * Implemented `try-with-resources` for all `java.lang.AutoCloseable` instances
    * Removed calls to `printStackTrace()` or `System.out.println` in favor of proper logging
* Reformatted code and cleaned up imports based on my preferences and using IntelliJ
* Removed all calls to `assert`... `assert` is disabled by default thus providing little real value while making the code incredibly hard to read and unnecessarily verbose
* Removed considerable amount of dead code, configurations, detritus and other nasty accumulations
    * Removed IDE specific settings
    * Removed references to personal setups or cloud instances
    * Removed directories such as `run`, `tools`, `nbproject`, `matlab`, `traces`
    * Removed all references to `JPAB` benchmark, this project has not been updated since 2012
* Removed calls to `commit()` during `Loader` operations

## Benchmarks

### From Original Paper
* [AuctionMark](http://hstore.cs.brown.edu/projects/auctionmark/)
* [CH-benCHmark](http://www-db.in.tum.de/research/projects/CHbenCHmark/?lang=en), mixed workload based on `TPC-C` and `TPC-H`
* Epinions.com
* [LinkBench](http://people.cs.uchicago.edu/~tga/pubs/sigmod-linkbench-2013.pdf)
* Synthetic Resource Stresser 
* [SEATS](http://hstore.cs.brown.edu/projects/seats)
* [SIBench](http://sydney.edu.au/engineering/it/~fekete/teaching/serializableSI-Fekete.pdf)
* [SmallBank](http://ses.library.usyd.edu.au/bitstream/2123/5353/1/michael-cahill-2009-thesis.pdf)
* [TATP](http://tatpbenchmark.sourceforge.net/)
* [TPC-C](http://www.tpc.org/tpcc/)
* Twitter
* [Voter](https://github.com/VoltDB/voltdb/tree/master/examples/voter) (Japanese "American Idol")
* Wikipedia
* [YCSB](https://github.com/brianfrankcooper/YCSB)

### Added Later
* [TPC-H](http://www.tpc.org/tpch)
* [TPC-DS](http://www.tpc.org/tpcds)
* hyadapt
* NoOp

### Removed
* JPAB

## How to Build
comming soon

## How to Run
comming soon

## How to Add Support for a New Database
comming soon

## Known Issues

* `tpch` - references files and directory that don't exist.  not clear what they should be.  see https://relational.fit.cvut.cz/dataset/TPCH
    ```
    14:55:46,892 (TPCHLoader.java:484) ERROR - data/tpch1/customer.tbl (No such file or directory)
    java.io.FileNotFoundException: data/tpch1/customer.tbl (No such file or directory)
        at java.io.FileInputStream.open0(Native Method)
        at java.io.FileInputStream.open(FileInputStream.java:195)
        at java.io.FileInputStream.<init>(FileInputStream.java:138)
        at java.io.FileReader.<init>(FileReader.java:72)
        at com.oltpbenchmark.benchmarks.tpch.TPCHLoader$TableLoader.run(TPCHLoader.java:372)
        at java.lang.Thread.run(Thread.java:748)
    ```
* `tpcds` - doesnt have a sample config.  will probably need data like `tpch`.  see https://relational.fit.cvut.cz/dataset/TPCDS
* `seats` - having difficulty loading
    
    has difficulty executing statement in JDBC/Application code.  Executes fine using cockroachdb cli.

    ```
    Exception in thread "main" java.lang.RuntimeException: Failed to execute threads: Failed to load data files for scaling-sized table 'RESERVATION'
        at com.oltpbenchmark.util.ThreadUtil.run(ThreadUtil.java:298)
        at com.oltpbenchmark.util.ThreadUtil.runNewPool(ThreadUtil.java:262)
        at com.oltpbenchmark.api.BenchmarkModule.loadDatabase(BenchmarkModule.java:290)
        at com.oltpbenchmark.api.BenchmarkModule.loadDatabase(BenchmarkModule.java:258)
        at com.oltpbenchmark.DBWorkload.runLoader(DBWorkload.java:794)
        at com.oltpbenchmark.DBWorkload.main(DBWorkload.java:525)
    Caused by: java.lang.RuntimeException: Failed to load data files for scaling-sized table 'RESERVATION'
        at com.oltpbenchmark.benchmarks.seats.SEATSLoader.loadScalingTable(SEATSLoader.java:408)
        at com.oltpbenchmark.benchmarks.seats.SEATSLoader$9.load(SEATSLoader.java:279)
        at com.oltpbenchmark.api.Loader$LoaderThread.run(Loader.java:64)
        at com.oltpbenchmark.util.ThreadUtil$LatchRunnable.run(ThreadUtil.java:332)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
    Caused by: java.lang.RuntimeException: Failed to load table RESERVATION
        at com.oltpbenchmark.benchmarks.seats.SEATSLoader.loadTable(SEATSLoader.java:553)
        at com.oltpbenchmark.benchmarks.seats.SEATSLoader.loadScalingTable(SEATSLoader.java:406)
        ... 6 more
    Caused by: java.sql.BatchUpdateException: Batch entry 0 `INSERT INTO RESERVATION VALUES (0, 4503599627371744, 492670330078089, 0, 338.0, 1051990098, 1046090437, 639389407, 605043792, 357467881, 727315974, 663499642, 1044342416, 167213553),(1, 4503599627375502, 492670330078089, 1, 995.0, 841704669, 764929309, 899047421, 121769798, 323185465, 67783163, 404579994, 950214696, 447172762),(2, 26177172834092381, 1407601444602685, 0, 951.0, 196708676, 641544870, 257584080, 114595727, 111264471, 444961287, 731780069, 839518216, 408842926),(3, 26177172834092528, 1407601444602685, 1, 904.0, 330221635, 670280165, 410957172, 321942631, 1005345029, 343541520, 651319550, 987491565, 580342074),(4, 47006321110679957, 1407457564410500, 0, 983.0, 992140072, 274956915, 760306513, 741672770, 494776998, 715947519, 193668197, 168459806, 757832303),(5, 4503599627376250, 633423924560799, 0, 421.0, 480586965, 407896096, 1058371541, 473362929, 132999391, 969647311, 119902557, 1040350022, 398643714),(6, 4503599627374251, 633423924560799, 1, 107.0, 342228097, 318812081, 844243442, 1057400054, 916300970, 742106369, 263907213, 485190005, 40327184),(7, 47850746040812737, 774102359638932, 0, 687.0, 994531227, 464053383, 781989848, 436469186, 834822618, 33196494, 38666983, 714679113, 83564475),(8, 21673573206720992, 914952589362378, 0, 600.0, 704020148, 606142029, 325605259, 733282708, 791608464, 668379612, 75460892, 874572029, 1039840212),(9, 56013520365420604, 633545260385215, 0, 354.0, 907805714, 20784630, 506268136, 683404991, 794339912, 49498029, 429793313, 534044796, 38226140),(10, 65865144550293758, 563039077826940, 0, 891.0, 713352769, 230435396, 693786553, 910297103, 839165488, 881667456, 283548518, 303141642, 666578538),(11, 65865144550293969, 563039077826940, 1, 765.0, 300521473, 277977065, 512260371, 964470603, 667321013, 440190890, 906375675, 920439813, 422631519),(12, 21673573206720539, 1055793156931724, 0, 409.0, 74766466, 908347957, 421009473, 343567350, 831651284, 732759071, 755918397, 309994028, 330415772),(13, 21673573206722177, 1055793156931724, 1, 884.0, 345962259, 640743848, 889685383, 94651261, 449248861, 507652452, 532260117, 184523990, 823464398),(14, 56857945295552879, 1125923532473202, 0, 538.0, 295019130, 255092732, 350401259, 852782039, 495153052, 28192527, 471310133, 415160162, 544980974),(15, 26177172834091575, 844514052227208, 0, 916.0, 593580931, 523885335, 211358367, 695000451, 1045019179, 215857779, 503464337, 10749071, 491788256),(16, 41939771529889289, 985433003868648, 0, 683.0, 32975377, 611725240, 550398330, 400218071, 1050483155, 806640082, 72058291, 721135853, 536258992),(17, 41939771529890050, 985433003868648, 1, 651.0, 1068242287, 978143633, 381578848, 548586532, 758648628, 911573793, 429726331, 900498213, 1040380348),(18, 41939771529889655, 985433003868648, 2, 287.0, 787904915, 197014402, 340764188, 657763264, 39642751, 358490734, 417961362, 111383545, 238934989),(19, 66428094503715469, 1266797386597019, 0, 140.0, 1011830087, 1048818158, 1064100198, 622049735, 456576704, 132464156, 178192422, 92346718, 935535792),(20, 66428094503715194, 1266797386597019, 1, 312.0, 257144886, 255817771, 17800531, 711665646, 165909132, 917326472, 693054442, 269756036, 223060825),(21, 23362423066984801, 985246171710627, 0, 330.0, 976257989, 334113415, 841295391, 466069172, 100953915, 499727805, 966437815, 816172880, 649038559),(22, 23362423066984986, 985246171710627, 1, 893.0, 187512071, 215882710, 730346654, 218611666, 1036252352, 1041827657, 512732123, 389297776, 522327733),(23, 23362423066985450, 985246171710627, 2, 651.0, 741668176, 159182688, 762554279, 217682146, 670155221, 285108989, 397397188, 592003257, 134001690),(24, 23362423066985755, 985246171710627, 3, 848.0, 592674328, 1067592380, 117766741, 284975743, 406653504, 109454142, 783211381, 332596381, 986734274),(25, 26177172834091310, 985379315860172, 0, 827.0, 436869673, 381810338, 11358254, 807572042, 647881307, 275670550, 653599449, 1052407004, 48361239),(26, 6192449487634587, 985302005285274, 0, 939.0, 313407042, 825505448, 381947545, 538448789, 236927379, 321525259, 637283810, 872882369, 962218625),(27, 6192449487634575, 985302005285274, 1, 839.0, 585058162, 662200880, 297789837, 440035934, 185568657, 1009780907, 658438195, 929854300, 631052171),(28, 54606145481867278, 1477760810779373, 0, 418.0, 539763067, 827145795, 23510787, 1022311783, 784677843, 799314751, 336151119, 737718879, 771340592),(29, 54606145481867275, 1477760810779373, 1, 195.0, 15361278, 395073278, 317699544, 57978169, 859135769, 569218051, 93299857, 362892525, 310596711),(30, 54606145481867306, 1477760810779373, 2, 285.0, 1017885527, 823695553, 356731118, 532041238, 836286136, 1009632317, 589363686, 657265937, 608039658),(31, 54606145481867308, 1477760810779373, 3, 751.0, 956766998, 193547925, 932556453, 241348636, 732093627, 291127795, 1066450100, 379304284, 473540598),(32, 59109745109238811, 844461440795426, 0, 974.0, 410319815, 994857504, 409594104, 930725, 729997160, 185782375, 920210996, 758997691, 1012879387),(33, 59109745109238878, 844461440795426, 1, 629.0, 886833325, 416876400, 1066244037, 904365336, 186116397, 755606270, 463243062, 481094648, 383170255),(34, 4503599627373500, 844632162566880, 0, 591.0, 420615190, 310959366, 259135091, 469989679, 284499416, 856393188, 560231212, 627736278, 619982705),(35, 56857945295554863, 703914004612204, 0, 861.0, 291775977, 387621205, 72480017, 43547546, 562360839, 57201139, 323007736, 713902175, 140718066),(36, 56857945295553209, 703914004612204, 1, 335.0, 884732210, 760792036, 1013300383, 994381996, 938384670, 1021609801, 87913933, 928656951, 1049378033),(37, 17169973579350125, 563166850269298, 0, 349.0, 367427569, 434860513, 410611274, 1052806761, 387175326, 383968989, 963656377, 448971611, 401331477),(38, 17169973579350343, 563166850269298, 1, 770.0, 573246635, 577507331, 786881941, 196933104, 81512485, 933045486, 745034293, 193155227, 814022527),(39, 28428972647777069, 985379315991156, 0, 914.0, 537005032, 531422944, 2967943, 590075075, 871831209, 336012442, 510219110, 953063598, 1073419450),(40, 28428972647776414, 985379315991156, 1, 208.0, 81255002, 696220012, 22584232, 322434218, 53914201, 546287031, 1043027368, 575507461, 105493035),(41, 28428972647776454, 985379315991156, 2, 407.0, 145169305, 1018953278, 447946070, 559807413, 772656597, 418682500, 996169229, 159693550, 687022415),(42, 41658296553179400, 492616645149432, 0, 628.0, 408796843, 67318750, 250148614, 507774837, 479529812, 1029264572, 954393897, 406980061, 896634242),(43, 21673573206723429, 633590355542359, 0, 490.0, 122983791, 900637481, 259596731, 186229741, 324610880, 504253382, 318375154, 285067769, 82051258),(44, 21673573206721103, 633590355542359, 1, 861.0, 156060084, 945026808, 120015741, 801892801, 11221203, 790541351, 452002604, 132809540, 147763040),(45, 47287796087392139, 492721872175944, 0, 711.0, 817180442, 319130231, 347329962, 251942150, 581572184, 789651358, 129862527, 736784351, 648509218),(46, 47287796087391399, 492721872175944, 1, 469.0, 668885365, 318608923, 325088302, 814853054, 427513996, 1010601731, 630239927, 19798352, 593085710),(47, 70368744177664083, 985424415589568, 0, 101.0, 66694781, 55993098, 152424678, 25997076, 130158192, 377990429, 79027349, 913220654, 704974187),(48, 70368744177664403, 985424415589568, 1, 173.0, 809406730, 469521153, 644915186, 881489459, 170468061, 1018506750, 726899643, 317395958, 796659050),(49, 70368744177664640, 985424415589568, 2, 730.0, 1009409585, 25594040, 644879242, 87693195, 225773712, 305000789, 444657256, 420979996, 465947390),(50, 70368744177664362, 985424415589568, 3, 290.0, 107914700, 215253162, 537946923, 234096899, 506513246, 422759006, 1035823604, 882743037, 769578060),(51, 4503599627373288, 844620351407111, 0, 252.0, 515172496, 132665235, 902354365, 844007358, 987409079, 974861647, 177831217, 906242263, 973754701),(52, 4503599627372349, 844620351407111, 1, 435.0, 565501123, 71283650, 162585319, 212521105, 855040462, 4962785, 525335652, 1001007639, 923099365),(53, 40813871623045256, 422346685169897, 0, 941.0, 960847515, 183660877, 154475876, 306047726, 218095244, 111283100, 920110022, 962972114, 76922121),(54, 40813871623045152, 422346685169897, 1, 862.0, 442217247, 540797858, 430183663, 490324570, 174011423, 839993017, 31046516, 95887947, 568459356),(55, 40813871623045227, 422346685169897, 2, 302.0, 612314942, 717371375, 410297943, 330553288, 897274778, 387891195, 86937537, 920578590, 15840206),(56, 35465847065543179, 492740125098602, 0, 422.0, 1023204792, 413917305, 168804501, 294232702, 85262672, 77628460, 852129693, 960199986, 721046517),(57, 12947848928690177, 1337276723069138, 0, 364.0, 502509094, 969944217, 391971067, 1016941722, 836404959, 1018888571, 5643174, 662213377, 118899053),(58, 9570149208162337, 492832465388560, 0, 663.0, 366929194, 71796585, 667024617, 403862565, 334068534, 46451195, 598784454, 778882456, 1005716078),(59, 9570149208162608, 492832465388560, 1, 867.0, 961961496, 140835181, 89668616, 36524062, 358033039, 1009505059, 189332586, 167393468, 758734978),(60, 9570149208162626, 492832465388560, 2, 142.0, 973598697, 599957902, 143409655, 426526653, 561891824, 243464565, 220609458, 602823772, 167899138),(61, 9570149208163464, 492832465388560, 3, 764.0, 947058301, 87238478, 694701187, 368601827, 959305656, 468373147, 931927918, 238368822, 200007240),(62, 9570149208163175, 492832465388560, 4, 789.0, 176885285, 86420574, 64939608, 662719820, 196137475, 876452288, 791844730, 645620265, 470506807),(63, 80220368362536967, 1055614919197861, 0, 229.0, 521334955, 984329024, 115663344, 1056529184, 157091551, 1000048328, 483289769, 131382924, 107090459)` was aborted: An I/O error occurred while sending to the backend.  Call getNextException to see other errors in the batch.
        at org.postgresql.jdbc.BatchResultHandler.handleError(BatchResultHandler.java:148)
        at org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:486)
        at org.postgresql.jdbc.PgStatement.executeBatch(PgStatement.java:840)
        at org.postgresql.jdbc.PgPreparedStatement.executeBatch(PgPreparedStatement.java:1538)
        at com.oltpbenchmark.benchmarks.seats.SEATSLoader.loadTable(SEATSLoader.java:541)
        ... 7 more
    Caused by: org.postgresql.util.PSQLException: An I/O error occurred while sending to the backend.
        at org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:487)
        ... 10 more
    Caused by: java.io.EOFException
        at org.postgresql.core.PGStream.receiveChar(PGStream.java:308)
        at org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:1952)
        at org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:481)
        ... 10 more

    
    ```
* `linkbench` - loader needs to be fixed, wrong tables referenced in loader
    ```
    19:38:01,771 (BenchmarkModule.java:293) DEBUG - Using legacy LinkBenchLoader.load() method
    Exception in thread "main" java.lang.NullPointerException
        at com.oltpbenchmark.util.SQLUtil.getInsertSQL(SQLUtil.java:374)
        at com.oltpbenchmark.util.SQLUtil.getInsertSQL(SQLUtil.java:355)
        at com.oltpbenchmark.benchmarks.linkbench.LinkBenchLoader.load(LinkBenchLoader.java:54)
        at com.oltpbenchmark.api.BenchmarkModule.loadDatabase(BenchmarkModule.java:296)
        at com.oltpbenchmark.api.BenchmarkModule.loadDatabase(BenchmarkModule.java:258)
        at com.oltpbenchmark.DBWorkload.runLoader(DBWorkload.java:794)
        at com.oltpbenchmark.DBWorkload.main(DBWorkload.java:525)
    ```
* `hyadapt` - has no config
* `auctionmark` - not yet working; data loading issue; lots of resource leaks
    ```
    Exception in thread "main" 20:08:52,021 (AuctionMarkLoader.java:202) INFO  - *** START USERACCT_ITEM
    java.lang.RuntimeException: Failed to execute threads: Unexpected error while generating table data for 'CATEGORY'
        at com.oltpbenchmark.util.ThreadUtil.run(ThreadUtil.java:298)
        at com.oltpbenchmark.util.ThreadUtil.runNewPool(ThreadUtil.java:262)
        at com.oltpbenchmark.api.BenchmarkModule.loadDatabase(BenchmarkModule.java:290)
        at com.oltpbenchmark.api.BenchmarkModule.loadDatabase(BenchmarkModule.java:258)
        at com.oltpbenchmark.DBWorkload.runLoader(DBWorkload.java:794)
        at com.oltpbenchmark.DBWorkload.main(DBWorkload.java:525)
    Caused by: java.lang.RuntimeException: Unexpected error while generating table data for 'CATEGORY'
        at com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkLoader$AbstractTableGenerator.load(AuctionMarkLoader.java:410)
        at com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkLoader$CountdownLoaderThread.load(AuctionMarkLoader.java:151)
        at com.oltpbenchmark.api.Loader$LoaderThread.run(Loader.java:64)
        at com.oltpbenchmark.util.ThreadUtil$LatchRunnable.run(ThreadUtil.java:332)
        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)
        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)
        at java.lang.Thread.run(Thread.java:748)
    Caused by: java.sql.BatchUpdateException: Batch entry 0 INSERT INTO CATEGORY VALUES (0, 'Antiques', NULL),(1, 'Antiquities', 0),(3, 'Byzantine', 1),(4, 'Celtic', 1),(5, 'Egyptian', 1),(6, 'Far Eastern', 1),(7, 'Greek', 1),(8, 'Holy Land', 1),(9, 'Islamic', 1),(10, 'Near Eastern', 1),(11, 'Neolithic &amp; Paleolithic', 1),(17, 'Other', 1),(16, 'Price Guides &amp; Publications', 1),(15, 'Reproductions', 1),(12, 'Roman', 1),(13, 'South Italian', 1),(2, 'The Americas', 1),(14, 'Viking', 1),(18, 'Architectural &amp; Garden', 0),(19, 'Balusters', 18),(20, 'Barn Doors', 18),(21, 'Beams', 18),(22, 'Ceiling Tins', 18),(23, 'Chandeliers, Fixtures, Sconces', 18),(24, 'Columns &amp; Posts', 18),(25, 'Corbels', 18),(26, 'Doors', 18),(27, 'Finials', 18),(28, 'Fireplaces &amp; Mantels', 18),(29, 'Garden', 18),(30, 'Hardware', 18),(31, 'Door Bells &amp; Knockers', 30),(32, 'Door Knobs &amp; Handles', 30),(33, 'Door Plates &amp; Backplates', 30),(34, 'Drawer Pulls', 30),(35, 'Escutcheons &amp; Key Hole Covers', 30),(36, 'Heating Grates &amp; Vents', 30),(37, 'Hooks &amp; Brackets', 30),(38, 'Locks &amp; Keys', 30),(39, 'Nails', 30),(41, 'Other', 30),(40, 'Switch Plates &amp; Outlet Covers', 30),(56, 'Other', 18),(42, 'Pediments', 18),(43, 'Plumbing', 18),(55, 'Price Guides &amp; Publications', 18),(54, 'Reproductions', 18),(44, 'Signs', 18),(45, 'Stained Glass Windows', 18),(47, '1900-1940', 45),(48, '1940-Now', 45),(46, 'Pre-1900', 45),(49, 'Unknown', 45),(50, 'Stair &amp; Carpet Rods', 18),(51, 'Tiles', 18),(52, 'Weathervanes &amp; Lightning Rods', 18),(53, 'Windows, Sashes &amp; Locks', 18),(57, 'Asian Antiques', 0),(58, 'Burma', 57),(59, 'China', 57),(60, 'Amulets', 59),(61, 'Armor', 59),(62, 'Baskets', 59),(63, 'Bells', 59),(64, 'Bowls', 59),(65, 'Boxes', 59),(66, 'Bracelets', 59),(67, 'Brush Pots', 59),(68, 'Brush Washers', 59),(69, 'Cabinets', 59),(70, 'Chairs', 59),(71, 'Chests', 59),(72, 'Fans', 59),(73, 'Glasses &amp; Cups', 59),(74, 'Incense Burners', 59),(75, 'Ink Stones', 59),(76, 'Masks', 59),(77, 'Necklaces &amp; Pendants', 59),(113, 'Other', 59),(78, 'Paintings &amp; Scrolls', 59),(79, 'Plates', 59),(80, 'Pots', 59),(81, 'Rings', 59),(82, 'Robes &amp; Textiles', 59),(83, 'Seals', 59),(84, 'Snuff Bottles', 59),(85, 'Statues', 59),(86, 'Birds', 85),(87, 'Buddha', 85),(88, 'Dogs', 85),(89, 'Dragons', 85),(90, 'Elephants', 85),(91, 'Foo Dogs', 85),(92, 'Horses', 85),(93, 'Kwan-yin', 85),(94, 'Men, Women &amp; Children', 85),(95, 'Mice', 85),(96, 'Monkeys', 85),(107, 'Other', 85),(97, 'Oxen', 85),(98, 'Phoenix', 85),(99, 'Pigs', 85),(100, 'Rabbits', 85),(101, 'Rats', 85),(102, 'Roosters', 85),(103, 'Sheep', 85),(104, 'Snakes', 85),(105, 'Tigers', 85),(106, 'Turtles', 85),(108, 'Swords', 59),(109, 'Tables', 59),(110, 'Tea Caddies', 59),(111, 'Teapots', 59),(112, 'Vases', 59),(114, 'India', 57),(115, 'Japan', 57),(116, 'Armor', 115),(117, 'Bells', 115),(118, 'Bowls', 115),(119, 'Boxes', 115),(120, 'Dolls', 115),(121, 'Fans', 115),(122, 'Glasses &amp; Cups', 115),(123, 'Katana', 115),(124, 'Kimonos &amp; Textiles', 115),(125, 'Masks', 115),(126, 'Netsuke', 115),(136, 'Other', 115) was aborted: ERROR: foreign key violation: value [0] not found in category@primary [c_id] (txn=a30ca1f6-1737-4540-892e-5db8e164b748)  Call getNextException to see other errors in the batch.
        at org.postgresql.jdbc.BatchResultHandler.handleCompletion(BatchResultHandler.java:166)
        at org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:492)
        at org.postgresql.jdbc.PgStatement.executeBatch(PgStatement.java:840)
        at org.postgresql.jdbc.PgPreparedStatement.executeBatch(PgPreparedStatement.java:1538)
        at com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkLoader.generateTableData(AuctionMarkLoader.java:237)
        at com.oltpbenchmark.benchmarks.auctionmark.AuctionMarkLoader$AbstractTableGenerator.load(AuctionMarkLoader.java:408)
        ... 6 more
    Caused by: org.postgresql.util.PSQLException: ERROR: foreign key violation: value [0] not found in category@primary [c_id] (txn=a30ca1f6-1737-4540-892e-5db8e164b748)
        at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2440)
        at org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:2183)
        at org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:481)
        ... 10 more

    ```


