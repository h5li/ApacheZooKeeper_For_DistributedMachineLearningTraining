import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.data.Stat;

class PSManager implements DataCallback {
    public float[][] grid;
    public int numParams;
    public int numWorkers;
    public int numDone;

    public PSManager(int numWorkers) {
        this.grid = null;
        this.numParams = 0;
        this.numWorkers = numWorkers;
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        if (args.length < 2) {
            System.err.println("USAGE: PSManager numWorkers numEpochs [host:port ...]");
            System.exit(2);
        }

        int numWorkers = Integer.parseInt(args[0]);
        int numEpochs = Integer.parseInt(args[1]);
        String addrs = args[2];
        for (int i = 3; i < args.length; i++)
            addrs += "," + args[i];
        ZooKeeper zk = new ZooKeeper(addrs, 3000, null);
        PSManager manager = new PSManager(numWorkers);

        if (zk.exists("/m", false) == null)
            zk.create("/m", null, null, CreateMode.PERSISTENT);
        for (int i = 0; i < numWorkers; i++) {
            String path = "/w" + i;
            zk.create(path, null, null, CreateMode.PERSISTENT);
        }

        for (int k = 0; k < numEpochs; k++) {
            for (int i = 0; i < numWorkers; i++)
                zk.getData("/w" + i, true, manager, null);
            manager.numDone = 0;

            zk.create("/start" + k, null, null, CreateMode.PERSISTENT);
            if (k > 0)
                zk.delete("/end" + (k - 1), -1);
            while (manager.numDone != numWorkers);

            byte[] vector = new byte[manager.numParams * 4];
            for (int i = 0; i < manager.numParams; i++) {
                float sum = 0;
                for (int j = 0; j < numWorkers; j++)
                    sum += manager.grid[j][i];
                float val = sum / numWorkers;

                byte[] byteRep = ByteBuffer.allocate(4).putFloat(val).array();
                for (int j = 0; j < 4; j++)
                    vector[4 * i + j] = byteRep[j];
            }
            zk.setData("/m", vector, -1);
            for (int i = 0; i < numWorkers; i++) {
                while(zk.exists("/ack" + i, false) == null);
            }

            zk.delete("/start" + k, -1);
            zk.create("/end" + k, null, null, CreateMode.PERSISTENT);
        }
    }

    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        if (rc != Code.OK.intValue()) {
            System.out.println("Something's wrong");
            return;
        }

        if (this.grid == null) {
            this.numParams = data.length / 4;
            this.grid = new float[this.numWorkers][this.numParams];
        }
        for (int j = 0; j < this.numParams; j++)
            this.grid[this.numDone][j] = ByteBuffer.wrap(data, 4 * j, 4).getFloat();
        this.numDone++;
    }
}