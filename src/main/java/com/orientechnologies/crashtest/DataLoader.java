package com.orientechnologies.crashtest;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class DataLoader {
  private static final Logger logger = LogManager.getFormatterLogger(DataLoader.class);

  static final int    VERTEX_COUNT   = 10_000;
  static final String V_ID           = "id";
  static final String RING_IDS       = "ringIds";
  static final String RING_SIZES     = "ringSizes";
  static final String RING_ID        = "ringId";
  static final String CRASH_V        = "CrashV";
  static final String CRASH_E        = "CrashE";
  static final String DB_NAME        = "crashdb";
  static final String DATABASES_PATH = "target/databases";
  static final String DATABASES_URL  = "plocal:" + DATABASES_PATH;

  public static final String ADD_INDEX_FLAG          = "-addIndex";
  public static final String ADD_LUCENE_INDEX_FLAG   = "-addLuceneIndex";
  public static final String ADD_BINARY_RECORDS_FLAG = "-addBinaryRecords";

  public static final String RANDOM_VALUE_FIELD  = "randomValue";
  public static final String RANDOM_VALUES_FIELD = "randomValues";
  public static final String RANDOM_VALUE_INDEX  = "RandomValueIndex";
  public static final String RANDOM_VALUES_INDEX = "randomValuesIndex";
  public static final String BINARY_FIELD        = "binaryField";
  public static final String BINARY_FIELD_SIZE   = "binaryFieldSize";
  
  public static final String LUCENE_TEST_FIELD_PREFIX = "val_";
  public static final String LUCENE_TEST_FIELD_NAME = "LuceneTestFieldName";
  public static final String LUCENE_RANDOM_VAL_INDEX = CRASH_V + "." + LUCENE_TEST_FIELD_NAME;
  public static final String LUCENE_TEST_CONTROL_FIELD = "LuceneTestControlField";
//  public static final int LUCENE_DOCUMENTS_COUNT = 100_000;

  public static final AtomicBoolean generateOOM = new AtomicBoolean();

  public static void main(String[] args) throws Exception {
    logger.info("Parsing command lines");

    final Set<String> argSet = new HashSet<>(Arrays.asList(args));

    final boolean addIndex;
    final boolean addBinaryRecords;
    final boolean addLuceneIndex;

    if (argSet.contains(ADD_INDEX_FLAG)) {
      logger.info("Additional indexes will be added to the crash tests");
      addIndex = true;
    } else {
      addIndex = false;
    }
    
    if (argSet.contains(ADD_LUCENE_INDEX_FLAG)) {
      logger.info("Additional lucene indexes will be added to the crash tests");
      addLuceneIndex = true;
    } else {
      addLuceneIndex = false;
    }

    if (argSet.contains(ADD_BINARY_RECORDS_FLAG)) {
      logger.info("Binary records will be stored in edge records of rings");
      addBinaryRecords = true;
    } else {
      addBinaryRecords = false;
    }

    startHaltThread();
    startOOMThread();

    final AtomicBoolean stopFlag = new AtomicBoolean();
    final ExecutorService loaderService = Executors.newCachedThreadPool();

    try (OrientDB orientDB = new OrientDB(DATABASES_URL, OrientDBConfig.defaultConfig())) {
      if (orientDB.exists(DB_NAME)) {
        orientDB.drop(DB_NAME);
      }

      orientDB.create(DB_NAME, ODatabaseType.PLOCAL);

      try (final ODatabaseSession session = orientDB.open(DB_NAME, "admin", "admin")) {
        final OClass vCls = session.createVertexClass(CRASH_V);

        vCls.createProperty(V_ID, OType.INTEGER).createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
        vCls.createProperty(RING_IDS, OType.EMBEDDEDLIST);
        vCls.createProperty(RING_SIZES, OType.EMBEDDEDLIST);

        final OClass eCls = session.createEdgeClass(CRASH_E);
        eCls.createProperty(RING_ID, OType.LONG);

        if (addIndex) {
          eCls.createProperty(RANDOM_VALUE_FIELD, OType.INTEGER);
          eCls.createProperty(RANDOM_VALUES_FIELD, OType.EMBEDDEDLIST, OType.INTEGER);

          eCls.createIndex(RANDOM_VALUE_INDEX, OClass.INDEX_TYPE.NOTUNIQUE, RANDOM_VALUE_FIELD);
          eCls.createIndex(RANDOM_VALUES_INDEX, OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX, RANDOM_VALUES_FIELD);
        }
        
        if (addLuceneIndex){                              
          vCls.createProperty(LUCENE_TEST_FIELD_NAME, OType.STRING);                    
          OIndex<?> index = vCls.createIndex(LUCENE_RANDOM_VAL_INDEX, "FULLTEXT", null, null, "LUCENE", new String[] {LUCENE_TEST_FIELD_NAME});
          vCls.createProperty(LUCENE_TEST_CONTROL_FIELD, OType.STRING);
        }

        if (addBinaryRecords) {
          eCls.createProperty(BINARY_FIELD, OType.BINARY);
          eCls.createProperty(BINARY_FIELD_SIZE, OType.INTEGER);
        }

        addStopFileWatcher(stopFlag, loaderService);

        logger.info("Start vertex addition");
        for (int i = 0; i < VERTEX_COUNT; i++) {
          OVertex vertex = session.newVertex(vCls);
          vertex.setProperty("id", i);
          vertex.save();

          if (i > 0 && i % 100_000 == 0) {
            logger.info("%d vertexes were added", i);
          }

          if (stopFlag.get()) {
            logger.info("Load of vertexes is stopped by stop file");
            return;
          }
        }
      }

      final List<Future<Void>> futures = new ArrayList<>();
      try (final ODatabasePool pool = new ODatabasePool(orientDB, "crashdb", "admin", "admin")) {
        final AtomicLong idGen = new AtomicLong();

        logger.info("%d vertexes were created", VERTEX_COUNT);
        logger.info("Start rings creation");
        for (int i = 0; i < 8; i++) {
          futures.add(loaderService.submit(new Loader(pool, idGen, addIndex, addBinaryRecords, addLuceneIndex, stopFlag)));
        }

        for (Future<Void> future : futures) {
          try {
            future.get();
          } catch (Exception e) {
            logger.error("Exception in loader", e);
          }
        }

        stopFlag.set(true);
      }
    } finally {
      loaderService.shutdown();
    }

    logger.info("All loaders are finished, waiting for a shutdown");

    //noinspection InfiniteLoopStatement
    while (true) {
      Thread.sleep(60 * 1000);
    }
  }

  private static void startHaltThread() throws IOException {
    logger.info("Starting JVM halt thread");

    final ServerSocket serverSocket = new ServerSocket(1025, 0, InetAddress.getLocalHost());
    serverSocket.setReuseAddress(true);

    final Thread crashThread = new Thread(() -> {
      try {
        logger.info("Halt thread is listening for the signal");

        final Socket clientSocket = serverSocket.accept();
        final InputStream crashStream = clientSocket.getInputStream();

        //noinspection InfiniteLoopStatement
        while (true) {
          final int value = crashStream.read();

          if (value == 42) {
            logger.info("Halt signal is received, trying to halt JVM");
            Runtime.getRuntime().halt(-1);
          } else if (value == -1) {
            logger.info("End of stream is reached in halt thread");
            break;
          } else {
            logger.info("Unknown signal is received by halt thread, listening for next signal");
          }
        }

      } catch (IOException e) {
        logger.error("Error during listening for JVM halt signal", e);
      }
    });

    crashThread.setDaemon(true);
    crashThread.start();
  }

  private static void startOOMThread() throws IOException {
    logger.info("Starting OOM thread");

    final ServerSocket serverSocket = new ServerSocket(1036, 0, InetAddress.getLocalHost());
    serverSocket.setReuseAddress(true);

    final Thread crashThread = new Thread(() -> {
      try {
        logger.info("OOM thread is listening for the signal");

        final Socket clientSocket = serverSocket.accept();
        final InputStream oomStream = clientSocket.getInputStream();

        //noinspection InfiniteLoopStatement
        while (true) {
          final int value = oomStream.read();

          if (value == 42) {
            logger.info("OOM signal is received, trying to pollute the heap");
            generateOOM.set(true);
            break;
          } else if (value == -1) {
            logger.info("End of stream is reached in OOM thread");
            break;
          } else {
            logger.info("Unknown signal is received by OOM thread, listening for next signal");
          }
        }

      } catch (IOException e) {
        logger.error("Error during listening for OOM signal", e);
      }
    });

    crashThread.setDaemon(true);
    crashThread.start();
  }

  private static void addStopFileWatcher(AtomicBoolean stopFlag, ExecutorService loaderService) throws IOException {
    final WatchService watcher = FileSystems.getDefault().newWatchService();
    final Path buildDir = Paths.get("target");
    buildDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
    loaderService.submit(new StopFlagWatcher(watcher, buildDir.resolve("stop.txt"), stopFlag));
  }
}
