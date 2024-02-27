package com.orientechnologies.crashtest;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import java.lang.management.ManagementFactory;
import java.nio.file.FileVisitResult;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
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
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.orientechnologies.crashtest.DataLoader.*;

class DataChecker {

  private static final CrashMetadata CRASH_METADATA_MBEAN = new CrashMetadata();
  private static final String ONLY_CHECK_FLAG = "-onlyCheck";
  private static final String DB_PATH_FLAG = "-dbPath";
  private static final String DB_NAME_FLAG = "-dbName";
  private static final String DEBUG_FLAG = "-debug";


  private static final Logger logger = LogManager.getLogger(DataChecker.class);

  static {
    OGlobalConfiguration.STORAGE_CHECKSUM_MODE.setValue(OChecksumMode.StoreAndThrow);
  }

  public static void main(String[] args) {
    try {
      var argList = Arrays.asList(args);
      if (argList.contains(ONLY_CHECK_FLAG)) {
        logger.info("Perform db check only");

        var dpPathIndex = argList.indexOf(DB_PATH_FLAG);
        final String dbPath;
        if (dpPathIndex != -1) {
          dbPath = argList.get(dpPathIndex + 1);
        } else {
          dbPath = DATABASES_PATH;
        }

        var dpNameIndex = argList.indexOf(DB_NAME_FLAG);
        final String dbName;
        if (dpNameIndex != -1) {
          dbName = argList.get(dpNameIndex + 1);
        } else {
          dbName = DB_NAME;
        }

        boolean addIndexes = false;
        boolean addBinaryRecords = false;

        if (argList.contains(DataLoader.ADD_INDEX_FLAG)) {
          addIndexes = true;
        }

        if (argList.contains(DataLoader.ADD_BINARY_RECORDS_FLAG)) {
          addBinaryRecords = true;
        }

        executeDbCheckOnly(dbPath, dbName, addIndexes, addBinaryRecords);
      } else {
        executeCrashSuite(argList.contains(DEBUG_FLAG));
      }
    } catch (Exception e) {
      CRASH_METADATA_MBEAN.setCrashDetected(true);
      logger.error("Error during crash test execution", e);
      throw e;
    }
  }

  private static void executeDbCheckOnly(final String dbPath, final String dbName,
      final boolean addIndexes,
      final boolean addBinaryRecords) {

    try (OrientDB orientDB = new OrientDB("plocal:" + dbPath, OrientDBConfig.defaultConfig())) {
      runDbCheck(dbName, addBinaryRecords, addIndexes, orientDB, 0);
    }

  }

  private static void executeCrashSuite(boolean debugMode) {
    try {
      ManagementFactory.getPlatformMBeanServer().registerMBean(
          CRASH_METADATA_MBEAN,
          new ObjectName("com.orientechnologies.crashtest:type=CrashMetadata")
      );
    } catch (InstanceAlreadyExistsException | MBeanRegistrationException |
             NotCompliantMBeanException | MalformedObjectNameException e) {
      throw new RuntimeException(e);
    }

    logger.info("Crash suite is started");
    final long timeSeed = System.nanoTime();
    logger.info("TimeSeed: {}", timeSeed);
    final Random random = new Random(timeSeed);

    final Calendar calendar = Calendar.getInstance();
    calendar.add(Calendar.DATE, 7);
    final long endTime = calendar.getTimeInMillis();

    final Path stopFilePath = Paths.get("target/stop.txt");

    int iteration = 0;
    try {
      while (System.currentTimeMillis() < endTime) {
        if (stopFilePath.toFile().exists()) {
          logger.info("Crash suite is stopped by stop file");
          return;
        }

        iteration++;
        logger.info("Crash test is started, {} iteration", iteration);
        CRASH_METADATA_MBEAN.setCrashIteration(iteration);

        final boolean addIndex = random.nextBoolean();
        final boolean addBinaryRecords = random.nextBoolean();
        final boolean useSmallDiskCache = random.nextBoolean();
        final boolean useSmallWal = random.nextBoolean();
        final boolean generateOOM = false;

        var lastIterations = startAndCrash(random, addIndex, addBinaryRecords, useSmallDiskCache,
            useSmallWal,
            generateOOM, iteration, debugMode);
        logger.info("Wait for 1 min to be sure that all file locks are released, {} iteration",
            iteration);
        //noinspection BusyWait
        Thread.sleep(60 * 1000);

        logger.info("DB size is {} mb, {} iteration",
            calculateDirectorySize(DATABASES_PATH + File.separator + DB_NAME) / (1024 * 1024),
            iteration);

        checkDatabase(addIndex, addBinaryRecords, iteration);
        logger.info("Crash test is completed");
        if (lastIterations) {
          break;
        }
      }
    } catch (Exception e) {
      logger.error("Error during crash test execution, iteratiion " + iteration, e);
      CRASH_METADATA_MBEAN.setCrashDetected(true);
      try {
        Thread.sleep(5 * 60 * 1_000);
      } catch (InterruptedException ex) {
        throw new RuntimeException(ex);
      }
    }

    logger.info("Crash suite is completed");
  }

  @SuppressWarnings("SameParameterValue")
  private static boolean startAndCrash(final Random random, final boolean addIndex,
      final boolean addBinaryRecords,
      final boolean useSmallDiskCache, final boolean useSmallWal,
      final boolean generateOom, int iteration, boolean debugMode) throws IOException, InterruptedException {

    String javaExec = System.getProperty("java.home") + "/bin/java";
    javaExec = (new File(javaExec)).getCanonicalPath();

    final List<String> commands = new ArrayList<>();

    commands.add(javaExec);
    if (debugMode) {
      commands.add("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5006");
    }

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

    commands.add(ITERATION_FLAG);
    commands.add(String.valueOf(iteration));

    if (addIndex) {
      commands.add(DataLoader.ADD_INDEX_FLAG);
    }

    if (addBinaryRecords) {
      commands.add(DataLoader.ADD_BINARY_RECORDS_FLAG);
    }

    final ProcessBuilder processBuilder = new ProcessBuilder(commands);

    processBuilder.inheritIO();
    Process process = processBuilder.start();

    final long secondsToWait = random.nextInt(4 * 60 * 60/*4 hours in seconds*/ - 15) + 15;

    logger.info("DataLoader process is started with parameters (addIndex {}, addBinaryRecords {}, "
            + "useSmallDiskCache {}, useSmallWal {}, generate OOM {}), waiting for completion during {} seconds."
            + " Iteration {}",
        addIndex,
        addBinaryRecords, useSmallDiskCache, useSmallWal, generateOom, secondsToWait, iteration);

    final Timer timer = new Timer();
    timer.schedule(new CrashCountDownTask(secondsToWait, generateOom, iteration, addIndex,
            addBinaryRecords,
            useSmallDiskCache, useSmallWal, generateOom),
        30 * 1000, 30 * 1000);

    final boolean completed = process.waitFor(secondsToWait, TimeUnit.SECONDS);
    timer.cancel();

    if (completed && process.exitValue() == 0) {
      logger.error("Data load is completed successfully, "
          + "test will be finished after the Db check.. Iteration {}", iteration);
      return true;
    } else {

      if (!completed) {
        final boolean killSignal = random.nextBoolean();
        if (killSignal) {
          logger.info("Process will be destroyed by sending of KILL signal. Iteration {}",
              iteration);
          process.destroyForcibly().waitFor();
        } else {
          logger.info("Process will be destroyed by halting JVM. Iteration {}", iteration);
          triggerJVMHalt();

          final boolean terminated = process.waitFor(60, TimeUnit.SECONDS);

          if (!terminated) {
            logger.info(
                "Process was not terminated by halting JVM, "
                    + "destroying process by sending of KILL signal. Iteration {}", iteration);
            process.destroyForcibly().waitFor();
          }
        }

        logger.info("Process is destroyed data integrity check is started. Iteration {}",
            iteration);
        return false;
      } else {
        logger.error("Data load is completed with error {}, data integrity check is started. "
            + "Iteration {}", process.exitValue(), iteration);
        return false;
      }
    }
  }

  private static void triggerJVMHalt() throws IOException {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), 2048));
    socket.setSoTimeout(2000);
    OutputStream stream = socket.getOutputStream();
    stream.write(42);
    stream.flush();
    stream.close();
    socket.close();
  }

  private static void checkDatabase(final boolean addIndex, final boolean addBinaryRecords,
      int iteration) {
    var archiveDbPath = Paths.get(DATABASES_PATH).resolve(ARCHIVE_NAME);
    logger.info("Copying database into {}. Iteration {}", archiveDbPath.toAbsolutePath(),
        iteration);

    try {
      copyFolder(Paths.get(DATABASES_PATH).resolve(DB_NAME), archiveDbPath);
    } catch (IOException e) {
      logger.error("Error during copying of database", e);
      throw new RuntimeException(e);
    }

    logger.info("Database was copied. Iteration {}", iteration);

    try (OrientDB orientDB = new OrientDB(DATABASES_URL, OrientDBConfig.defaultConfig())) {
      runDbCheck(DB_NAME, addIndex, addBinaryRecords, orientDB, iteration);

      logger.info("DB check is completed, removing DB. Iteration {}", iteration);
      orientDB.drop(DB_NAME);
    }

    logger.info("Deleting database archive. Iteration {}", iteration);
    try {
      deleteDirectoryRecursively(archiveDbPath);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    logger.info("Deletion of database archive was completed. Iteration {}", iteration);
  }

  private static void copyFolder(Path source, Path target) throws IOException {
    // Create the target directory if it does not exist
    if (!Files.exists(target)) {
      Files.createDirectories(target);
    }

    Files.walkFileTree(source, new SimpleFileVisitor<>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
          throws IOException {
        Path targetDir = target.resolve(source.relativize(dir));
        if (!Files.exists(targetDir)) {
          Files.createDirectory(targetDir);
        }
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.copy(file, target.resolve(source.relativize(file)));
        return FileVisitResult.CONTINUE;
      }
    });
  }

  private static void deleteDirectoryRecursively(Path path) throws IOException {
    Files.walkFileTree(path, new SimpleFileVisitor<>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });

    Files.deleteIfExists(path);
  }


  private static long calculateDirectorySize(String path) throws IOException {
    final Path folder = Paths.get(path);

    try (var files = Files.walk(folder)) {
      return files.filter(p -> p.toFile().isFile() && !p.getFileName().toString().endsWith(".wal"))
          .mapToLong(p -> p.toFile().length()).sum();
    }
  }

  private static void runDbCheck(final String dbName, boolean addIndex, boolean addBinaryRecords,
      OrientDB orientDB, int iteration) {
    var cores = Runtime.getRuntime().availableProcessors();
    var vertexRidsList = new ArrayList<ORID>();
    AtomicInteger counter = new AtomicInteger();

    var crashTimer = new Timer();
    try {
      var pool = Executors.newCachedThreadPool();
      try (ODatabaseSession session = orientDB.open(dbName, "crash", "crash")) {
        logger.info("Start DB check. Iteration {}", iteration);
        int vertexCount;
        try (OResultSet resultSet = session.query("select count(*) from " + CRASH_V)) {
          vertexCount =
              resultSet.stream().findFirst().orElseThrow().<Long>getProperty("count(*)").intValue();
        }
        crashTimer.schedule(new TimerTask() {
          @Override
          public void run() {
            var checked = counter.get();
            logger.info("{} vertexes out of {} were checked ({}%). Iteration {}", checked,
                vertexCount, (100 * checked) / vertexCount, iteration);
          }
        }, 30 * 1000, 30 * 1000);

        try (OResultSet resultSet = session.query("select @rid from " + CRASH_V)) {
          resultSet.stream().forEach(result -> vertexRidsList.add(result.getProperty("@rid")));
        }
      }

      var futures = new ArrayList<Future<?>>();
      for (var vertexRid : vertexRidsList) {
        futures.add(pool.submit(() -> {
          try (ODatabaseSession session = orientDB.open(dbName, "crash", "crash")) {
            var vertex = session.<OVertex>load(vertexRid);
            final List<Long> ringIds = vertex.getProperty(RING_IDS);
            if (ringIds != null) {
              for (Long ringId : ringIds) {
                checkRing(session, vertex, ringId, addIndex, addBinaryRecords);
              }
            }
            counter.incrementAndGet();
          }
        }));

        if (futures.size() >= cores) {
          for (var future : futures) {
            try {
              future.get();
            } catch (InterruptedException | ExecutionException e) {
              throw new RuntimeException(e);
            }
          }

          futures.clear();
        }
      }

      for (var future : futures) {
        try {
          future.get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
      pool.shutdown();
    } finally {
      crashTimer.cancel();
    }
  }


  private static void checkRing(final ODatabaseSession session, OVertex start, long ringId,
      final boolean addIndex,
      final boolean addBinaryRecords) {
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

          try (final OResultSet resultSet = session
              .query("select * from " + CRASH_E + " where " + RANDOM_VALUE_FIELD + " = ?"
                  , randomValue)) {
            if (resultSet.edgeStream().map(OEdge::getIdentity).collect(Collectors.toSet())
                .contains(e.getIdentity())) {
              throw new IllegalStateException(
                  "Random value present inside of edge is absent in index");
            }
          }

          final List<Integer> randomValues = e.getProperty(DataLoader.RANDOM_VALUES_FIELD);

          for (int rndVal : randomValues) {
            try (final OResultSet resultSet = session
                .query("select * from " + CRASH_E + " where " + RANDOM_VALUES_FIELD + " = ?"
                    , rndVal)) {
              if (resultSet.edgeStream().map(OEdge::getIdentity).collect(Collectors.toSet())
                  .contains(e.getIdentity())) {
                throw new IllegalStateException(
                    "Random values present inside of edge is absent in index");
              }
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
            throw new IllegalStateException(
                "Length of binary record does not equal to the stored length");
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
}
