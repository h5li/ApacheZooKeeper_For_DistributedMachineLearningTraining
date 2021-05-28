import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;

class PSManager {
    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        if (args.length < 1) {
            System.err.println("USAGE: Executor [host:port ...]");
            System.exit(2);
        }

        String addrs = args[0];
        for (int i = 1; i < args.length; i++)
            addrs += "," + args[i];

        // 1. create the znodes
        ZooKeeper zk = new ZooKeeper(addrs, 3000, null);
        int done = 0;
        for (String arg : args)
            zk.create("/w", null, null, CreateMode.EPHEMERAL_SEQUENTIAL);
        zk.create("/m", null, null, CreateMode.PERSISTENT);

        // 2. get the data once training is completed
        float[][] grid = null;
        int numParams = 0;
        for (int i = 0; i < args.length; i++) {
            byte[] params = zk.getData("/w" + i, true, null);
            if (grid == null) {
                numParams = params.length / 4;
                grid = new float[args.length][numParams];
            }
            for (int j = 0; j < params.length; i += 4) {
                grid[i][j] = ByteBuffer.wrap(params, i, 4).getFloat();
            }
        }

        // 3. write the average to the znode
        byte[] vector = new byte[numParams * 4];
        for (int i = 0; i < numParams; i++) {
            float sum = 0;
            for (int j = 0; j < args.length; j++)
                sum += grid[j][i];
            float val = sum / args.length;

            byte[] byteRep = ByteBuffer.allocate(4).putFloat(val).array();
            for (int j = 0; j < 4; j++)
                vector[4 * i + j] = byteRep[j];
        }
        zk.setData("/m", vector, -1);
    }
}