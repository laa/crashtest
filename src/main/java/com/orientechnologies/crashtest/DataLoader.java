package com.orientechnologies.crashtest;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.sun.jna.Pointer;
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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class DataLoader {
  private static final Logger logger = LogManager.getFormatterLogger(DataLoader.class);

  static final int    VERTEX_COUNT  = 10_000_000;
  static final String V_ID          = "id";
  static final String RING_IDS      = "ringIds";
  static final String RING_SIZES    = "ringSizes";
  static final String RING_ID       = "ringId";
  static final String CRASH_V       = "CrashV";
  static final String CRASH_E       = "CrashE";
  static final String DB_NAME       = "crashdb";
  static final String DATABASES_DIR = "plocal:target/databases";

  public static void main(String[] args) throws Exception {
    startCrashThread();

    final AtomicBoolean stopFlag = new AtomicBoolean();
    final ExecutorService loaderService = Executors.newCachedThreadPool();

    try (OrientDB orientDB = new OrientDB(DATABASES_DIR, OrientDBConfig.defaultConfig())) {
      if (orientDB.exists("crashdb")) {
        orientDB.drop("crashdb");
      }

      orientDB.create(DB_NAME, ODatabaseType.PLOCAL);

      try (final ODatabaseSession session = orientDB.open("crashdb", "admin", "admin")) {
        final OClass vCls = session.createVertexClass(CRASH_V);

        vCls.createProperty(V_ID, OType.INTEGER).createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
        vCls.createProperty(RING_IDS, OType.EMBEDDEDLIST);
        vCls.createProperty(RING_SIZES, OType.EMBEDDEDLIST);

        final OClass eCls = session.createEdgeClass(CRASH_E);
        eCls.createProperty(RING_ID, OType.LONG);

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
          futures.add(loaderService.submit(new Loader(pool, idGen, stopFlag)));
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

    final ServerSocket serverSocket = new ServerSocket(777, 0, InetAddress.getLocalHost());
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
