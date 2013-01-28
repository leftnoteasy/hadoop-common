package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.client.ClientService;

public class URMContainerAllocator extends RMContainerAllocator {

  public URMContainerAllocator(ClientService clientService, AppContext context) {
    super(clientService, context);
  }

}
