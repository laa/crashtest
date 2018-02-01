package com.orientechnologies.crashtest;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
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

public class DataLoader {
  private static final Logger logger = LogManager.getFormatterLogger(DataLoader.class);

  static final int    VERTEX_COUNT   = 10_000_000;
  static final String V_ID           = "id";
  static final String RING_IDS       = "ringIds";
  static final String RING_SIZES     = "ringSizes";
  static final String RING_ID        = "ringId";
  static final String CRASH_V        = "CrashV";
  static final String CRASH_E        = "CrashE";
  static final String DB_NAME        = "crashdb";
  static final String DATABASES_PATH = "target/databases";
  static final String DATABASES_URL  = "plocal:" + DATABASES_PATH;

  public static final String ADD_INDEX_FLAG            = "-addIndex";
  public static final String ADD_BINARY_RECORDS_FLAG   = "-addBinaryRecords";
  public static final String USE_SMALL_DISK_CACHE_FLAG = "-useSmallDiskCache";
  public static final String USE_SMALL_WALL            = "-useSmallWall";
  public static final String RANDOM_VALUE_FIELD        = "randomValue";
  public static final String RANDOM_VALUES_FIELD       = "randomValues";
  public static final String RANDOM_VALUE_INDEX        = "RandomValueIndex";
  public static final String RANDOM_VALUES_INDEX       = "randomValuesIndex";
  public static final String BINARY_FIELD              = "binaryField";
  public static final String BINARY_FIELD_SIZE         = "binaryFieldSize";

  public static void main(String[] args) throws Exception {
    logger.info("Parsing command lines");

    final Set<String> argSet = new HashSet<>(Arrays.asList(args));

    final boolean addIndex;
    final boolean addBinaryRecords;
    final boolean useSmallDiskCache;
    final boolean useSmallWal;

    if (argSet.contains(ADD_INDEX_FLAG)) {
      logger.info("Additional indexes will be added to the crash tests");
      addIndex = true;
    } else {
      addIndex = false;
    }

    if (argSet.contains(ADD_BINARY_RECORDS_FLAG)) {
      logger.info("Binary records will be stored in edge records of rings");
      addBinaryRecords = true;
    } else {
      addBinaryRecords = false;
    }

    if (argSet.contains(USE_SMALL_DISK_CACHE_FLAG)) {
      logger.info("Size of disk cache will be limited to 4 gigabytes");
      useSmallDiskCache = true;
    } else {
      useSmallDiskCache = false;
    }

    if (argSet.contains(USE_SMALL_WALL)) {
      logger.info("Size of wal will be limited to 4GB");
      useSmallWal = true;
    } else {
      useSmallWal = false;
    }

    if (useSmallDiskCache) {
      OGlobalConfiguration.DISK_CACHE_SIZE.setValue(4 * 1024);
    }

    if (useSmallWal) {
      OGlobalConfiguration.WAL_MAX_SIZE.setValue(4096);
    }

    startCrashThread();

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
          futures.add(loaderService.submit(new Loader(pool, idGen, addIndex, addBinaryRecords, stopFlag)));
        }

        for (Future<Void> future : futures) {
          future.get();
        }

        stopFlag.set(true);
      }
    } finally {
      loaderService.shutdown();
    }
  }

  private static void startCrashThread() throws IOException {
    logger.info("Starting JVM halt thread");

    final ServerSocket serverSocket = new ServerSocket(1025, 0, InetAddress.getLocalHost());
    serverSocket.setReuseAddress(true);

    final Thread crashThread = new Thread(() -> {
      try {
        logger.info("Halt thread is listening for the signal");

        final Socket clientSocket = serverSocket.accept();
        final InputStream crashStream = clientSocket.getInputStream();

        while (true) {
          final int value = crashStream.read();

          if (value == 42) {
            logger.info("Halt signal is received, trying to halt JVM");
            Runtime.getRuntime().halt(-1);
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

  private static void addStopFileWatcher(AtomicBoolean stopFlag, ExecutorService loaderService) throws IOException {
    final WatchService watcher = FileSystems.getDefault().newWatchService();
    final Path buildDir = Paths.get("target");
    buildDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
    loaderService.submit(new StopFlagWatcher(watcher, buildDir.resolve("stop.txt"), stopFlag));
  }
}
