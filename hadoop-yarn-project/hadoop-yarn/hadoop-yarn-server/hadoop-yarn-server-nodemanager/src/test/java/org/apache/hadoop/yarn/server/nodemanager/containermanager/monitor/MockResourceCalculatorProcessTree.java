package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.yarn.util.ResourceCalculatorProcessTree;

public class MockResourceCalculatorProcessTree extends ResourceCalculatorProcessTree {
  long pmem = 0;
  long vmem = 0;
  
  public MockResourceCalculatorProcessTree(String root) {
    super(root);
  }

  @Override
  public void updateProcessTree() {
  }

  @Override
  public String getProcessTreeDump() {
    return "";
  }

  @Override
  public long getCumulativeVmem(int olderThanAge) {
    return olderThanAge > 0 ? 0 : vmem;
  }

  @Override
  public long getCumulativeRssmem(int olderThanAge) {
    return olderThanAge > 0 ? 0 : pmem;
  }

  @Override
  public long getCumulativeCpuTime() {
    return 0;
  }

  @Override
  public boolean checkPidPgrpidForMatch() {
    return true;
  }
  
  public void setPmem(long pmem) {
    this.pmem = pmem;
  }
  
  public void setVmem(long vmem) {
    this.vmem = vmem;
  }
}
