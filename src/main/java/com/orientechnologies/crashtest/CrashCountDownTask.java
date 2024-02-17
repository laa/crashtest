package com.orientechnologies.crashtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.TimerTask;

class CrashCountDownTask extends TimerTask {

  private static final Logger logger = LogManager.getLogger(CrashCountDownTask.class);

  private final long crashTs;
  private final boolean generateOOM;
  private boolean oomWasTriggered;

  private final int iteration;


  private final boolean addIndex;

  private final boolean addBinaryRecords;
  private final boolean useSmallDiskCache;
  private final boolean useSmallWal;
  private final boolean generateOom;

  CrashCountDownTask(long secondsToWait, boolean generateOOM, int iteration, boolean addIndex,
      boolean addBinaryRecords, boolean useSmallDiskCache, boolean useSmallWal,
      boolean generateOom) {
    this.crashTs = System.currentTimeMillis() + secondsToWait * 1000;
    this.generateOOM = generateOOM;
    this.iteration = iteration;
    this.addIndex = addIndex;
    this.addBinaryRecords = addBinaryRecords;
    this.useSmallDiskCache = useSmallDiskCache;
    this.useSmallWal = useSmallWal;
    this.generateOom = generateOom;
  }

  @Override
  public void run() {
    long interval = crashTs - System.currentTimeMillis();
    //generate oom before 5 minutes left to the crash
    if (generateOOM && !oomWasTriggered && interval < 5 * 60 * 1000) {
      try {
        logger.info("Triggering OOM signal");
        oomWasTriggered = true;
        triggerOOM();
      } catch (IOException e) {
        logger.error("Error during sending of OOM signal", e);
      }
    }

    //more than one second left
    if (interval > 1000) {
      final int hh = (int) (interval / (60 * 60 * 1000));
      interval -= hh * (60 * 60 * 1000);

      final int mm = (int) (interval / (60 * 1000));
      interval -= mm * (60 * 1000);

      final int ss = (int) (interval / 1000);
      logger.info("{}:{}:{} left till database crash:"
              + " (addIndex {}, addBinaryRecords {}, useSmallDiskCache {},"
              + " useSmallWal {}, generate OOM {}). Iteration {}", hh, mm, ss,
          addIndex,
          addBinaryRecords, useSmallDiskCache, useSmallWal, generateOom,
          iteration);
    }
  }

  private void triggerOOM() throws IOException {
    final Socket socket = new Socket();
    socket.connect(new InetSocketAddress(InetAddress.getLocalHost(), 1036));
    socket.setSoTimeout(2000);
    OutputStream stream = socket.getOutputStream();
    stream.write(42);
    stream.flush();
    stream.close();
    socket.close();
  }
}
