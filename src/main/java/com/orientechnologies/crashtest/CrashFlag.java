package com.orientechnologies.crashtest;

public class CrashFlag implements CrashFlagMBean {
  private volatile  boolean crashDetected;

  @Override
  public boolean isCrashDetected() {
    return crashDetected;
  }

  public void setCrashDetected(boolean crashDetected) {
    this.crashDetected = crashDetected;
  }
}
