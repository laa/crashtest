package com.orientechnologies.crashtest;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.List;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.orientechnologies.crashtest.DataLoader.CRASH_E;
import static com.orientechnologies.crashtest.DataLoader.CRASH_V;
import static com.orientechnologies.crashtest.DataLoader.DATABASES_DIR;
import static com.orientechnologies.crashtest.DataLoader.DB_NAME;
import static com.orientechnologies.crashtest.DataLoader.RING_ID;
import static com.orientechnologies.crashtest.DataLoader.RING_IDS;
import static com.orientechnologies.crashtest.DataLoader.RING_SIZES;

public class DataChecker {
  private static final Logger logger = LogManager.getFormatterLogger(DataChecker.class);

  public static void main(String[] args) {
    logger.info("Crash suite is started");
    final long timeSeed = System.nanoTime();
    logger.info("TimeSeed: %d", timeSeed);
    final Random random = new Random(timeSeed);

    OGlobalConfiguration.STORAGE_CHECKSUM_MODE.setValue(OChecksumMode.StoreAndThrow);

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
        if (startAndCrash(random)) {
          logger.info("Wait for 15 min to be sure that all file locks are released");
          Thread.sleep(15 * 60 * 1000);
          checkDatabase();
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

  private static boolean startAndCrash(final Random random) throws IOException, InterruptedException {

    String javaExec = System.getProperty("java.home") + "/bin/java";
    javaExec = (new File(javaExec)).getCanonicalPath();
    final ProcessBuilder processBuilder = new ProcessBuilder(javaExec, "-Xmx2048m", "-classpath",
        System.getProperty("java.class.path"), DataLoader.class.getName());
    processBuilder.inheritIO();
    Process process = processBuilder.start();

    final long secondsToWait = random.nextInt(24 * 60 * 60 /*24 hours in seconds*/ - 15) + 15;

    logger.info("DataLoader process is started, waiting for completion during %d seconds...", secondsToWait);
    final Timer timer = new Timer();
    timer.schedule(new CrashCountDownTask(secondsToWait), 30 * 1000, 30 * 1000);

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
    socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), 777));
    socket.setSoTimeout(2000);
    socket.getOutputStream().write(42);
  }

  private static void checkDatabase() {
    try (OrientDB orientDB = new OrientDB(DATABASES_DIR, OrientDBConfig.defaultConfig())) {
      try (ODatabaseSession session = orientDB.open(DB_NAME, "admin", "admin")) {

        AtomicInteger counter = new AtomicInteger();
        logger.info("Start DB check");
        try (OResultSet resultSet = session.query("select from " + CRASH_V)) {
          resultSet.vertexStream().forEach(v -> {
            final List<Long> ringIds = v.getProperty(RING_IDS);
            if (ringIds != null) {
              for (Long ringId : ringIds) {
                checkRing(v, ringId);
              }
            }

            final int cnt = counter.incrementAndGet();
            if (cnt > 0 && cnt % 1000 == 0) {
              logger.info("%d vertexes were checked", cnt);
            }
          });
        }
      }

      logger.info("DB check is completed, removing DB");

      orientDB.drop(DB_NAME);
    }
  }

  private static void checkRing(OVertex start, long ringId) {
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
      }

      if (vCount > ringSize) {
        throw new IllegalStateException("Invalid ring size value");
      }
    } while (!v.equals(start));

    if (vCount != ringSize) {
      throw new IllegalStateException("Expected and actual ring sizes are not equal");
    }
  }
}
