package com.orientechnologies.crashtest;

public class CrashMetadata implements CrashMetadataMBean {
  private volatile  boolean crashDetected;

  private volatile int crashIteration;

  @Override
  public boolean isCrashDetected() {
    return crashDetected;
  }

  @Override
  public int getCrashIteration() {
    return crashIteration;
  }

  public void setCrashDetected(boolean crashDetected) {
    this.crashDetected = crashDetected;
  }

  public void setCrashIteration(int crashIteration) {
    this.crashIteration = crashIteration;
  }
}
