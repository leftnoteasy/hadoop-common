package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.yarn.util.ResourceCalculatorPlugin;

public class MockResourceCalculatorPlugin extends ResourceCalculatorPlugin {
  @Override
  public long getVirtualMemorySize() {
    return 0;
  }

  @Override
  public long getPhysicalMemorySize() {
    return 0;
  }

  @Override
  public long getAvailableVirtualMemorySize() {
    return 0;
  }

  @Override
  public long getAvailablePhysicalMemorySize() {
    return 0;
  }

  @Override
  public int getNumProcessors() {
    return 0;
  }

  @Override
  public long getCpuFrequency() {
    return 0;
  }

  @Override
  public long getCumulativeCpuTime() {
    return 0;
  }

  @Override
  public float getCpuUsage() {
    return 0;
  }
}