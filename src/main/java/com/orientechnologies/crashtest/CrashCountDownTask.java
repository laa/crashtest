package com.orientechnologies.crashtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.TimerTask;

public class CrashCountDownTask extends TimerTask {
  private static final Logger logger = LogManager.getFormatterLogger(CrashCountDownTask.class);

  private final long crashTs;

  CrashCountDownTask(long secondsToWait) {
    this.crashTs = System.currentTimeMillis() + secondsToWait * 1000;
  }

  @Override
  public void run() {
    long interval = crashTs - System.currentTimeMillis();
    //more than one second left
    if (interval > 1000) {
      final int hh = (int) (interval / (60 * 60 * 1000));
      interval -= hh * (60 * 60 * 1000);

      final int mm = (int) (interval / (60 * 1000));
      interval -= mm * (60 * 1000);

      final int ss = (int) (interval / 1000);
      logger.info("%02d:%02d:%02d left till database crash", hh, mm, ss);

    }
  }
}
