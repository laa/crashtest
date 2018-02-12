package com.orientechnologies.crashtest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

class StopFlagWatcher implements Callable<Void> {
  private static final Logger logger = LogManager.getFormatterLogger(StopFlagWatcher.class);

  private final WatchService  watcher;
  private final Path          stopFile;
  private final AtomicBoolean stopFlag;

  StopFlagWatcher(WatchService watcher, Path stopFile, AtomicBoolean stopFlag) {
    this.watcher = watcher;
    this.stopFile = stopFile;
    this.stopFlag = stopFlag;
  }

  @Override
  public Void call() throws Exception {
    logger.info("Start watching for stop.txt file");
    try {
      while (!stopFlag.get()) {
        final WatchKey watchKey = watcher.take();

        for (WatchEvent<?> event : watchKey.pollEvents()) {
          final WatchEvent.Kind<?> kind = event.kind();
          if (kind == StandardWatchEventKinds.OVERFLOW) {
            if (stopFile.toFile().exists()) {
              logger.info("Stop file was detected, process will be stopped");
              stopFlag.set(true);
              return null;
            }
          } else if (kind == StandardWatchEventKinds.ENTRY_CREATE || kind == StandardWatchEventKinds.ENTRY_MODIFY) {
            final Path fileName = (Path) event.context();
            if (fileName.getFileName().equals(stopFile.getFileName())) {
              logger.info("Stop file was detected, process will be stopped");
              stopFlag.set(true);
              return null;
            }
          } else {
            logger.error("Invalid event kind was detected %s", kind);
          }
        }

        final boolean valid = watchKey.reset();
        if (!valid) {
          logger.error("WatchKey is not valid anymore, stop file will not be monitored any longer");
          return null;
        }
      }
    } catch (Exception e) {
      logger.error("Error during watching of stop.txt file, please stop process manually when needed", e);
      throw e;
    }

    return null;
  }
}
