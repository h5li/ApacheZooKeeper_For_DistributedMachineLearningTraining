import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.data.Stat;

class PSWorker implements DataCallback {
    public ZooKeeper zk;
    public int id;

    public PSWorker(String addrs, int id) throws KeeperException, IOException {
        this.zk = new ZooKeeper(addrs, 3000, null);
        this.id = id;
    }
    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        if (args.length < 2) {
            System.err.println("USAGE: PSWorker workerNumber numEpochs dataFile [host:port ...]");
            System.exit(2);
        }

        int workerId = Integer.parseInt(args[0]);
        int numEpochs = Integer.parseInt(args[1]);
        String addrs = args[3];
        for (int i = 4; i < args.length; i++)
            addrs += "," + args[i];
        PSWorker worker = new PSWorker(addrs, workerId);
        
        for(int k = 0; k < numEpochs; k++) {
            while(worker.zk.exists("/start" + k, false) == null);
            worker.zk.getData("/m", true, worker, null);

            ProcessBuilder pb = new ProcessBuilder("python", "compute_gradient.py", args[2]);
            Process process = pb.start();
            if (process.waitFor() != 0)
                return;

            List<Float> grads = new ArrayList<Float>();
            try {
                File file = new File("grads.txt");
                Scanner scanner = new Scanner(file);
                while (scanner.hasNextFloat()) {
                    grads.add(scanner.nextFloat());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            byte[] vector = new byte[grads.size() * 4];
            for (int i = 0; i < grads.size(); i++) {
                byte[] byteRep = ByteBuffer.allocate(4).putFloat(grads.get(i)).array();
                for (int j = 0; j < 4; j++)
                    vector[4 * i + j] = byteRep[j];
            }
            worker.zk.setData("/w" + workerId, vector, -1);

            while(worker.zk.exists("/end" + k, false) == null);
            worker.zk.delete("/ack" + worker.id, -1);
        }
    }

    // 4. Get the average gradient from the manager znode
    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        if (rc != Code.OK.intValue()) {
            System.out.println("Something's wrong");
            return;
        }

        try {
            FileWriter fw = new FileWriter(new File("grads.txt"), false);
            for (int j = 0; j < data.length / 4; j++) {
                fw.write(ByteBuffer.wrap(data, 4 * j, 4).getFloat() + "\n");
            }
            fw.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        try {
            ProcessBuilder pb = new ProcessBuilder("python", "update_params.py");
            Process process = pb.start();
            if (process.waitFor() != 0)
                return;
            this.zk.create("/ack" + this.id, null, null, CreateMode.PERSISTENT);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }
}