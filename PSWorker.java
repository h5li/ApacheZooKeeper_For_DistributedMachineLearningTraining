import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;

class PSWorker {
    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        if (args.length < 2) {
            System.err.println("USAGE: PSWorker workerNumber [host:port ...]");
            System.exit(2);
        }

        int workerId = Integer.parseInt(args[0]);
        String addrs = args[1];
        for (int i = 2; i < args.length; i++)
            addrs += "," + args[i];
        ZooKeeper zk = new ZooKeeper(addrs, 3000, null);

        // 1. Execute training script
        // 2. Read params and convert to byte array
        // 3. Write the data to znode with same workerId
        // 4. Get the average gradient from the manager znode
    }
}