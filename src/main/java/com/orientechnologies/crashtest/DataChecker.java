package com.orientechnologies.crashtest;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static com.orientechnologies.crashtest.DataLoader.CRASH_E;
import static com.orientechnologies.crashtest.DataLoader.CRASH_V;
import static com.orientechnologies.crashtest.DataLoader.DATABASES_PATH;
import static com.orientechnologies.crashtest.DataLoader.DATABASES_URL;
import static com.orientechnologies.crashtest.DataLoader.DB_NAME;
import static com.orientechnologies.crashtest.DataLoader.RING_ID;
import static com.orientechnologies.crashtest.DataLoader.RING_IDS;
import static com.orientechnologies.crashtest.DataLoader.RING_SIZES;
import com.orientechnologies.orient.core.id.ORID;
import java.util.Objects;

class DataChecker {
  private static final String ONLY_CHECK_FLAG = "-onlyCheck";

  private static final Logger logger = LogManager.getFormatterLogger(DataChecker.class);

  private static final Path backupPath = Paths.get("/media/hdd/crash_backup");

  static {
    OGlobalConfiguration.STORAGE_CHECKSUM_MODE.setValue(OChecksumMode.StoreAndThrow);
  }

  public static void main(String[] args) {
    if (args.length > 0 && args[0].equals(ONLY_CHECK_FLAG)) {
      logger.info("Perform db check only");

      final String dbPath = args[1];
      final String dbName = args[2];

      final Set<String> argSet = new HashSet<>(Arrays.asList(args).subList(3, args.length));

      boolean addIndexes = false;
      boolean addBinaryRecords = false;
      boolean addLuceneIndex = false;
      
      if (argSet.contains(DataLoader.ADD_INDEX_FLAG)) {
        addIndexes = true;
      }

      if (argSet.contains(DataLoader.ADD_BINARY_RECORDS_FLAG)) {
        addBinaryRecords = true;
      }
      
      if (argSet.contains(DataLoader.ADD_LUCENE_INDEX_FLAG)) {
        addLuceneIndex = true;
      }

      executeDbCheckOnly(dbPath, dbName, addIndexes, addBinaryRecords, addLuceneIndex);
    } else {
      executeCrashSuite();
    }
  }

  private static void executeDbCheckOnly(final String dbPath, final String dbName, final boolean addIndexes,
      final boolean addBinaryRecords, final boolean addLuceneIndex) {

    try (OrientDB orientDB = new OrientDB("plocal:" + dbPath, OrientDBConfig.defaultConfig())) {
      runDbCheck(dbName, addBinaryRecords, addIndexes, addLuceneIndex, orientDB);
    }

  }

  private static void executeCrashSuite() {
    logger.info("Crash suite is started");
    final long timeSeed = System.nanoTime();
    logger.info("TimeSeed: %d", timeSeed);
    final Random random = new Random(timeSeed);

    final Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, 7);
    final long endTime = calendar.getTimeInMillis();

    final Path stopFilePath = Paths.get("target/stop.txt");

    int counter = 0;
    try {
      while (System.currentTimeMillis() < endTime) {
        if (stopFilePath.toFile().exists()) {
          logger.info("Crash suite is stopped by stop file");
          return;
        }

        counter++;
        logger.info("Crash test is started, %d iteration", counter);

        final boolean addIndex = random.nextBoolean();
        final boolean addBinaryRecords = random.nextBoolean();
        final boolean addLuceneIndex = true;//random.nextBoolean();
        final boolean useSmallDiskCache = random.nextBoolean();
        final boolean useSmallWal = random.nextBoolean();
        final boolean generateOOM = false;

        if (startAndCrash(random, addIndex, addBinaryRecords, addLuceneIndex, useSmallDiskCache, useSmallWal, generateOOM)) {
          logger.info("Wait for 1 min to be sure that all file locks are released");
          Thread.sleep(60 * 1000);

          logger.info("DB size is %d mb", calculateDirectorySize(DATABASES_PATH + File.separator + DB_NAME) / (1024 * 1024));

          logger.info("Perform database backup to the %s", backupPath);
          if (Files.exists(backupPath)) {
            OFileUtils.deleteRecursively(backupPath.toFile());
          }

          Files.createDirectories(backupPath);
          copyFolder(new File(DATABASES_PATH + File.separator + DB_NAME), backupPath.toFile());
          logger.info("Backup is completed");


          checkDatabase(addIndex, addBinaryRecords, addLuceneIndex);
        } else {
          return;
        }
        logger.info("Crash test is completed");
      }
    } catch (Exception e) {
      logger.error("Error during crash test execution", e);
    }

    logger.info("Crash suite is completed");
  }

  private static boolean startAndCrash(final Random random, final boolean addIndex, final boolean addBinaryRecords,
      final boolean addLuceneindex,
      final boolean useSmallDiskCache, final boolean useSmallWal, final boolean generateOOM)
      throws IOException, InterruptedException {

    String javaExec = System.getProperty("java.home") + "/bin/java";
    javaExec = (new File(javaExec)).getCanonicalPath();

    final List<String> commands = new ArrayList<>();

    commands.add(javaExec);
    commands.add("-Xmx2048m");

    if (useSmallDiskCache) {
      commands.add("-Dstorage.diskCache.bufferSize=4096");
    }

    if (useSmallWal) {
      commands.add("-Dstorage.wal.maxSize=4096");
    }

    commands.add("-classpath");
    commands.add(System.getProperty("java.class.path"));

    commands.add(DataLoader.class.getName());

    if (addIndex) {
      commands.add(DataLoader.ADD_INDEX_FLAG);
    }

    if (addBinaryRecords) {
      commands.add(DataLoader.ADD_BINARY_RECORDS_FLAG);
    }
    
    if (addLuceneindex){
      commands.add(DataLoader.ADD_LUCENE_INDEX_FLAG);
    }

    final ProcessBuilder processBuilder = new ProcessBuilder(commands);

    processBuilder.inheritIO();
    Process process = processBuilder.start();

    final long secondsToWait = random.nextInt( 4 * 60 * 60/*4 hours in seconds*/ - 15) + 15;

    logger.info("DataLoader process is started with parameters (addIndex %b, addBinaryRecords %b, "
            + "useSmallDiskCache %b, useSmallWal %b, generate OOM %b), waiting for completion during %d seconds...", addIndex,
        addBinaryRecords, useSmallDiskCache, useSmallWal, generateOOM, secondsToWait);

    final Timer timer = new Timer();
    timer.schedule(new CrashCountDownTask(secondsToWait, generateOOM), 30 * 1000, 30 * 1000);

    final boolean completed = process.waitFor(secondsToWait, TimeUnit.SECONDS);
    if (completed) {
      timer.cancel();
      logger.error("Data load is completed successfully nothing to check");
      return false;
    } else {
      timer.cancel();

      final boolean killSignal = random.nextBoolean();

      if (killSignal) {
        logger.info("Process will be destroyed by sending of KILL signal");
        process.destroyForcibly().waitFor();
      } else {
        logger.info("Process will be destroyed by halting JVM");
        triggerJVMHalt();

        final boolean terminated = process.waitFor(60, TimeUnit.SECONDS);

        if (!terminated) {
          logger.info("Process was not terminated by halting JVM, destroying process by sending of KILL signal");
          process.destroyForcibly().waitFor();
        }
      }

      logger.info("Process is destroyed data integrity check is started");
      return true;
    }
  }

  private static void triggerJVMHalt() throws IOException {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), 1025));
    socket.setSoTimeout(2000);
    OutputStream stream = socket.getOutputStream();
    stream.write(42);
    stream.flush();
    stream.close();
    socket.close();
  }

  private static void checkDatabase(final boolean addIndex, final boolean addBinaryRecords, final boolean addLuceneIndex) throws IOException {
    try (OrientDB orientDB = new OrientDB(DATABASES_URL, OrientDBConfig.defaultConfig())) {
      runDbCheck(DB_NAME, addIndex, addBinaryRecords, addLuceneIndex, orientDB);

      logger.info("DB check is completed, removing DB");
      orientDB.drop(DB_NAME);
    }
  }

  private static long calculateDirectorySize(String path) throws IOException {
    final Path folder = Paths.get(path);
    return Files.walk(folder).filter(p -> p.toFile().isFile() && !p.getFileName().toString().endsWith(".wal"))
        .mapToLong(p -> p.toFile().length()).sum();
  }

  private static void runDbCheck(final String dbName, boolean addIndex, boolean addBinaryRecords, 
          final boolean addLuceneIndex, OrientDB orientDB) {
    try (ODatabaseSession session = orientDB.open(dbName, "admin", "admin")) {
      final OIndexManager indexManager = session.getMetadata().getIndexManager();
      final OIndex<Set<OIdentifiable>> randomValueIndex;
      final OIndex<Set<OIdentifiable>> randomValuesIndex;
      final OIndex<Set<OIdentifiable>> luceneRandomValueIndex;

      if (addIndex) {
        //noinspection unchecked
        randomValueIndex = (OIndex<Set<OIdentifiable>>) indexManager.getIndex(DataLoader.RANDOM_VALUE_INDEX);
        //noinspection unchecked
        randomValuesIndex = (OIndex<Set<OIdentifiable>>) indexManager.getIndex(DataLoader.RANDOM_VALUES_INDEX);
      } else {
        randomValueIndex = null;
        randomValuesIndex = null;
      }
      
      if (addLuceneIndex){
        luceneRandomValueIndex = (OIndex<Set<OIdentifiable>>) indexManager.getIndex(DataLoader.LUCENE_RANDOM_VAL_INDEX);
      }
      else{
        luceneRandomValueIndex = null;
      }

      AtomicInteger counter = new AtomicInteger();
      logger.info("Start DB check");
      try (OResultSet resultSet = session.query("select from " + CRASH_V)) {
        resultSet.vertexStream().forEach(v -> {
          final List<Long> ringIds = v.getProperty(RING_IDS);
          if (ringIds != null) {
            for (Long ringId : ringIds) {
              checkRing(v, ringId, addIndex, randomValueIndex, randomValuesIndex, addBinaryRecords);
            }
          }
          
          checkLuceneIndexValues(v, luceneRandomValueIndex);

          final int cnt = counter.incrementAndGet();
          if (cnt > 0 && cnt % 1000 == 0) {
            logger.info("%d vertexes were checked", cnt);
          }
        });
      }
      
      checkLuceneIndexPairs(session, luceneRandomValueIndex);
    }
  }

  private static void pack(String sourceDirPath, String zipFilePath) throws IOException {
    final Path p = Files.createFile(Paths.get(zipFilePath));

    try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(p))) {
      Path pp = Paths.get(sourceDirPath);
      Files.walk(pp).forEach(path -> {
        ZipEntry zipEntry = new ZipEntry(pp.relativize(path).toString());
        try {
          if (!Files.isDirectory(path)) {
            zs.putNextEntry(zipEntry);
            Files.copy(path, zs);
          }
        } catch (IOException e) {
          throw new IllegalStateException(e);
        }
      });
    }
  }

  private static void checkRing(OVertex start, long ringId, final boolean addIndex, OIndex<Set<OIdentifiable>> randomValueIndex,
      OIndex<Set<OIdentifiable>> randomValuesIndex, final boolean addBinaryRecords) {
    int vCount = 1;
    OVertex v = start;

    final List<Long> rIds = start.getProperty(RING_IDS);
    final int idIndex = rIds.indexOf(ringId);
    final List<Integer> ringSizes = start.getProperty(RING_SIZES);
    final int ringSize = ringSizes.get(idIndex);

    do {
      for (OEdge e : v.getEdges(ODirection.OUT, CRASH_E)) {
        if (e.getProperty(RING_ID).equals(ringId)) {
          v = e.getTo();

          if (!v.equals(start)) {
            vCount++;

            final List<Long> ringIds = v.getProperty(RING_IDS);
            final int ringIdIndex = ringIds.indexOf(ringId);
            final List<Integer> rSizes = v.getProperty(RING_SIZES);
            final int rSize = rSizes.get(ringIdIndex);

            if (rSize != ringSize) {
              throw new IllegalStateException("Expected and actual ring sizes are not equal");
            }
          }
          break;
        }

        if (addIndex) {
          final int randomValue = e.getProperty(DataLoader.RANDOM_VALUE_FIELD);

          Set<OIdentifiable> edges = randomValueIndex.get(randomValue);
          if (!edges.contains(e.getIdentity())) {
            throw new IllegalStateException("Random value present inside of edge is absent in index");
          }

          final List<Integer> randomValues = e.getProperty(DataLoader.RANDOM_VALUES_FIELD);

          for (int rndVal : randomValues) {
            edges = randomValuesIndex.get(rndVal);

            if (!edges.contains(e.getIdentity())) {
              throw new IllegalStateException("Random values present inside of edge is absent in index");
            }
          }
        }

        if (addBinaryRecords) {
          final byte[] binaryRecord = e.getProperty(DataLoader.BINARY_FIELD);

          if (binaryRecord == null) {
            throw new IllegalStateException("Binary record is absent");
          }

          final int binaryRecordLength = e.getProperty(DataLoader.BINARY_FIELD_SIZE);

          if (binaryRecord.length != binaryRecordLength) {
            throw new IllegalStateException("Length of binary record does not equal to the stored length");
          }
        }

      }

      if (vCount > ringSize) {
        throw new IllegalStateException("Invalid ring size value");
      }
    } while (!v.equals(start));

    if (vCount != ringSize) {
      throw new IllegalStateException("Expected and actual ring sizes are not equal");
    }
  }

  static private void copyFolder(File src, File dest) throws IOException {
    if (src.listFiles() == null || src.listFiles().length == 0)
      return;

    if (!dest.exists())
      Files.createDirectories(dest.toPath());

    for (File file : src.listFiles()) {
      File fileDest = new File(dest, file.getName());
      if (file.isDirectory()) {
        copyFolder(file, fileDest);
      } else {
        if (fileDest.exists())
          continue;
        Files.copy(file.getAbsoluteFile().toPath(), fileDest.getAbsoluteFile().toPath());
      }
    }
  }

  private static void checkLuceneIndexValues(OVertex v, OIndex<Set<OIdentifiable>> luceneRandomValueIndex) {    
    if (luceneRandomValueIndex == null){
      return;
    }
    
    ORID rid = v.getIdentity();
    String controlValue = v.getProperty(DataLoader.LUCENE_TEST_CONTROL_FIELD);
    Set<OIdentifiable> indexedrecords = luceneRandomValueIndex.get(controlValue);
    if (!indexedrecords.contains(rid)){
      throw new IllegalStateException("Control value not found in Lucene index");
    }
    
  }
  
  /**
   * check if each value from index has equal value in database
   * @param db
   * @param luceneRandomValueIndex 
   */
  private static void checkLuceneIndexPairs(ODatabaseSession db, OIndex<Set<OIdentifiable>> luceneRandomValueIndex){
    if (luceneRandomValueIndex == null){
      return;
    }    
    String indexName = luceneRandomValueIndex.getName();
    String query = "SELECT COUNT(*) as cnt FROM " + DataLoader.CRASH_V + " WHERE SEARCH_INDEX(\"" + indexName + "\", \"" + DataLoader.LUCENE_TEST_FIELD_PREFIX + "*\") = true";
    OResultSet rs = db.query(query, (Object[])null);
    Integer countInIndex = null;
    if (rs.hasNext()){
      countInIndex = rs.next().getProperty("cnt");      
    }
    rs.close();
    
    query = "SELECT COUNT(*) as cnt FROM " + DataLoader.CRASH_V + " WHERE " + DataLoader.LUCENE_TEST_CONTROL_FIELD + " IS NOT NULL";
    Integer documentWithControlFieldSetCount = null;
    if (rs.hasNext()){
      documentWithControlFieldSetCount = rs.next().getProperty("cnt");      
    }
    rs.close();
    
    if (!Objects.equals(countInIndex, documentWithControlFieldSetCount)){
      throw new IllegalStateException("Number of control values doesn't match number of index entries");
    }
  }
}
