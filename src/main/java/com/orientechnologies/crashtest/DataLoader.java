package com.orientechnologies.crashtest;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class DataLoader {

  static {
    OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.setValue(4 *1024);
    OGlobalConfiguration.SBTREE_MAX_KEY_SIZE.setValue(1500);

    System.setProperty("java.util.logging.manager", "org.apache.logging.log4j.jul.LogManager");
  }

  private static final Logger logger = LogManager.getLogger(DataLoader.class);

  static final int VERTEX_COUNT = 10_000_000;
  static final String V_ID = "id";
  static final String RING_IDS = "ringIds";
  static final String RING_SIZES = "ringSizes";
  static final String RING_ID = "ringId";
  static final String CRASH_V = "CrashV";
  static final String CRASH_E = "CrashE";
  static final String DB_NAME = "crashdb";

  static final String ARCHIVE_NAME = "crashdb_archive";
  static final String DATABASES_PATH = "target/databases";
  static final String DATABASES_URL = "plocal:" + DATABASES_PATH;

  public static final String ADD_INDEX_FLAG = "-addIndex";
  public static final String ADD_BINARY_RECORDS_FLAG = "-addBinaryRecords";

  public static final String ITERATION_FLAG = "-iteration";

  public static final String RANDOM_VALUE_FIELD = "randomValue";
  public static final String RANDOM_VALUES_FIELD = "randomValues";
  public static final String RANDOM_VALUE_INDEX = "RandomValueIndex";
  public static final String RANDOM_VALUES_INDEX = "randomValuesIndex";
  public static final String BINARY_FIELD = "binaryField";
  public static final String BINARY_FIELD_SIZE = "binaryFieldSize";

  public static int iteration = 0;

  public static final AtomicBoolean generateOOM = new AtomicBoolean();

  public static void main(String[] args) throws Exception {

    logger.info("Parsing command lines");

    var argList = Arrays.asList(args);

    final boolean addIndex;
    final boolean addBinaryRecords;

    var iterationIndex = argList.indexOf(ITERATION_FLAG);
    if (iterationIndex >= 0) {
      iteration = Integer.parseInt(argList.get(iterationIndex + 1));
    }

    if (argList.contains(ADD_INDEX_FLAG)) {
      logger.info("Additional indexes will be added to the crash tests. Iteration {}",
          iteration);
      addIndex = true;
    } else {
      addIndex = false;
    }

    if (argList.contains(ADD_BINARY_RECORDS_FLAG)) {
      logger.info("Binary records will be stored in edge records of rings. Iteration {}",
          iteration);
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

      orientDB.execute("create database " + DB_NAME +
          " plocal users (crash identified by 'crash' role admin)");

      final int vertexesToAdd =
          ThreadLocalRandom.current().nextInt(VERTEX_COUNT - 100_000) + 100_000;

      try (final ODatabaseSession session = orientDB.open(DB_NAME, "crash", "crash")) {
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
          eCls.createIndex(RANDOM_VALUES_INDEX, OClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX,
              RANDOM_VALUES_FIELD);
        }

        if (addBinaryRecords) {
          eCls.createProperty(BINARY_FIELD, OType.BINARY);
          eCls.createProperty(BINARY_FIELD_SIZE, OType.INTEGER);
        }

        addStopFileWatcher(stopFlag, loaderService);

        logger.info("Start vertex addition, {} vertexes will be created. Iteration {}",
            vertexesToAdd, iteration);
        for (int i = 0; i < vertexesToAdd; i++) {
          OVertex vertex = session.newVertex(vCls);
          vertex.setProperty("id", i);
          vertex.save();

          if (stopFlag.get()) {
            logger.info("Load of vertexes is stopped by stop file. Iteration {}", iteration);
            return;
          }
        }
      }

      final List<Future<Void>> futures = new ArrayList<>();
      try (final ODatabasePool pool = new ODatabasePool(orientDB, "crashdb", "crash",
          "crash")) {
        final AtomicLong idGen = new AtomicLong();

        logger.info("{} vertexes were created. Iteration {}", vertexesToAdd, iteration);
        var threadsCount = Math.max(Runtime.getRuntime().availableProcessors(), 8);
        logger.info("Start rings creation using {} loaders. Iteration {}", threadsCount,
            iteration);
        for (int i = 0; i < threadsCount; i++) {
          futures.add(loaderService.submit(
              new Loader(pool, idGen, addIndex, addBinaryRecords, stopFlag, vertexesToAdd,
                  iteration)));
        }

        for (Future<Void> future : futures) {
          try {
            future.get();
          } catch (Exception e) {
            logger.error("Exception in loader. Iteration " + iteration, e);
          }
        }

        stopFlag.set(true);
      }
    } finally {
      loaderService.shutdown();
    }

    logger.info("All loaders are finished, waiting for a shutdown. Iteration {}", iteration);

    //noinspection InfiniteLoopStatement
    while (true) {
      //noinspection BusyWait
      Thread.sleep(60 * 1000);
    }
  }

  private static void startHaltThread() throws IOException {
    logger.info("Starting JVM halt thread. Iteration {}", iteration);

    @SuppressWarnings("resource") final ServerSocket serverSocket = new ServerSocket(2048, 1, null);
    serverSocket.setReuseAddress(true);

    final Thread crashThread = new Thread(() -> {
      try {
        logger.info("Halt thread is listening for the signal. Iteration {}", iteration);

        while (true) {
          final Socket clientSocket = serverSocket.accept();
          final InputStream crashStream = clientSocket.getInputStream();

          while (true) {
            final int value = crashStream.read();

            if (value == 42) {
              logger.info("Halt signal is received, trying to halt JVM. Iteration {}", iteration);
              Runtime.getRuntime().halt(-1);
            } else if (value == -1) {
              logger.info("End of stream is reached in halt thread. Iteration {}", iteration);
              break;
            } else {
              logger.info("Unknown signal is received {} by halt thread,"
                      + " listening for next signal. Iteration {}",
                  value, iteration);
            }
          }
        }

      } catch (IOException e) {
        logger.error("Error during listening for JVM halt signal. Iteration " + iteration, e);
      }
    });

    crashThread.setDaemon(true);
    crashThread.start();
  }

  private static void startOOMThread() throws IOException {
    logger.info("Starting OOM thread. Iteration {}", iteration);

    @SuppressWarnings("resource") final ServerSocket serverSocket = new ServerSocket(1036, 1, null);
    serverSocket.setReuseAddress(true);

    final Thread crashThread = new Thread(() -> {
      try {
        logger.info("OOM thread is listening for the signal. Iteration {}", iteration);

        final Socket clientSocket = serverSocket.accept();
        final InputStream oomStream = clientSocket.getInputStream();

        while (true) {
          final int value = oomStream.read();

          if (value == 42) {
            logger.info("OOM signal is received, trying to pollute the heap. Iteration {}",
                iteration);
            generateOOM.set(true);
            break;
          } else if (value == -1) {
            logger.info("End of stream is reached in OOM thread. Iteration {}", iteration);
            break;
          } else {
            logger.info("Unknown signal is received by OOM thread {}, listening for next signal",
                value);
          }
        }

      } catch (IOException e) {
        logger.error("Error during listening for OOM signal. Iterations " + iteration, e);
      }
    });

    crashThread.setDaemon(true);
    crashThread.start();
  }

  private static void addStopFileWatcher(AtomicBoolean stopFlag, ExecutorService loaderService)
      throws IOException {
    final WatchService watcher = FileSystems.getDefault().newWatchService();
    final Path buildDir = Paths.get("target");
    buildDir.register(watcher, StandardWatchEventKinds.ENTRY_CREATE,
        StandardWatchEventKinds.ENTRY_MODIFY);
    loaderService.submit(new StopFlagWatcher(watcher, buildDir.resolve("stop.txt"), stopFlag));
  }
}
