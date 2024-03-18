package org.apache.iotdb.mq;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.rpc.DeepCopyRpcTransportFactory;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.service.rpc.thrift.IClientRPCService;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class MQSession {
  private IClientRPCService.Iface client;
  private TTransport transport;

  public MQSession() {}

  public void initClient(TEndPoint endPoint) {
    DeepCopyRpcTransportFactory.setDefaultBufferCapacity(IoTDBMQConstant.DEFAULT_BUFFER_SIZE);
    DeepCopyRpcTransportFactory.setThriftMaxFrameSize(IoTDBMQConstant.DEFAULT_MAX_FRAME_SIZE);

    try {
      transport =
          DeepCopyRpcTransportFactory.INSTANCE.getTransport(
              // as there is a try-catch already, we do not need to use TSocket.wrap
              endPoint.getIp(), endPoint.getPort(), IoTDBMQConstant.DEFAULT_CONNECTION_TIMEOUT_MS);

      if (!transport.isOpen()) {
        transport.open();
      }
    } catch (TTransportException e) {
      throw new RuntimeException(e);
    }

    client = new IClientRPCService.Client(new TBinaryProtocol(transport));
    client = RpcUtils.newSynchronizedClient(client);

    //        client.pipeSubscribe()

  }
}
