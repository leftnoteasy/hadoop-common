package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.yarn.api.records.ContainerId;

public class ContainerChangeMonitoringEvent extends ContainersMonitorEvent {
  private final long vmemLimit;
  private final long pmemLimit;
  
  public ContainerChangeMonitoringEvent(ContainerId containerId,
      long vmemLimit, long pmemLimit) {
    super(containerId, ContainersMonitorEventType.CHANGE_MONITORING_CONTAINER);
    this.vmemLimit = vmemLimit;
    this.pmemLimit = pmemLimit;
  }
  
  public long getVmemLimit() {
    return this.vmemLimit;
  }

  public long getPmemLimit() {
    return this.pmemLimit;
  }
}
