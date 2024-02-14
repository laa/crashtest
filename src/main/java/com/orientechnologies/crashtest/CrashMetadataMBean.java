package com.orientechnologies.crashtest;

public interface CrashMetadataMBean {
  @SuppressWarnings("unused")
  boolean isCrashDetected();

  @SuppressWarnings("unused")
  int getCrashIteration();
}
