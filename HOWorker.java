import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.*;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.ACL;

class HOWorker implements Watcher, StatCallback {
    public ZooKeeper zk;
    public int id;
    public int numWorker;
    public List<Double> grads;
    public int curr_iter;
    public List<Double> previousGrad;
    public static final List<ACL> OPEN_ACL_UNSAFE = new LinkedList<ACL>();

    public HOWorker(String addrs, int id, int numWorker) throws KeeperException, IOException {
        this.zk = new ZooKeeper(addrs, 3000, this);
        this.id = id;
        this.numWorker = numWorker;
        this.grads = null;
        this.curr_iter = 0;
	    this.previousGrad = null;
        OPEN_ACL_UNSAFE.add(new ACL(Perms.ALL, Ids.ANYONE_ID_UNSAFE));
    }

    public static void main(String[] args) throws KeeperException, InterruptedException, IOException {
        if (args.length < 5) {
            System.err.println("USAGE: HOWorker workerNumber numWorker numEpochs dataFile [host:port ...]");
            System.exit(2);
        }

        int workerId = Integer.parseInt(args[0]);
        int numEpochs = Integer.parseInt(args[2]);
        int numWorker = Integer.parseInt(args[1]);
        String addrs = args[4];
        for (int i = 5; i < args.length; i++)
            addrs += "," + args[i];
        HOWorker worker = new HOWorker(addrs, workerId, numWorker);
	    System.out.println("Worker " + workerId + " started");
        
        if (worker.zk.exists("/w" + workerId, false) == null) {
            worker.zk.create("/w" + workerId, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        for (int i = 0; i < numWorker; i++) {
            while (worker.zk.exists("/w" + i, false) == null);
        }
        
        for(int k = 0; k < numEpochs; k++) {
	        System.out.println("Worker " + workerId + " started epoch" + k);
            worker.curr_iter = k;
	        worker.grads = null;
            worker.previousGrad = null;
            int previousWorkerIdToListen = worker.getWorkerIdToListen();
            worker.zk.exists("/w" + previousWorkerIdToListen, true, worker, null);

            ProcessBuilder pb = new ProcessBuilder("python", "compute_gradient.py", args[3], ""+workerId);
            Process process = pb.start();
            if (process.waitFor() != 0) {
		        System.out.println("Python Process exited unexpectedly");
                return;
	        }

            List<Double> grads = new ArrayList<Double>();
            try {
                File file = new File("grads"+workerId+".txt");
                Scanner scanner = new Scanner(file);
                while (scanner.hasNextDouble()) {
                    grads.add(scanner.nextDouble());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            worker.grads = grads;
            // String path = "/compute" + worker.id + "w" + worker.curr_iter;
            // if (worker.zk.exists(path, false) == null)
            //     worker.zk.create(path, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            // for (int i = 0; i < numWorker; i++) {
            //     while (worker.zk.exists("/compute" + i + "w" + k, false) == null) {
            // 	    // worker.zk.exists("/w" + previousWorkerIdToListen, true, worker, null);
		    //         System.out.println("Worker " + workerId + " Waiting for worker " + i + " to finish TRAINING" + k);
		    //         Thread.sleep(1000);
		    //     }
            // }
	        if (workerId == 0) {
                worker.writeGradToZnode(0);
            }
            else if(worker.previousGrad != null) {
                for (int i = 0; i < worker.previousGrad.size(); i++) {
                    worker.grads.set(i, worker.grads.get(i) + worker.previousGrad.get(i));
                }
                worker.writeGradToZnode(1);
                if (worker.id == numWorker - 1) {
                    worker.writeGradToFile();
                }
            }

	    
            for (int i = 0; i < numWorker; i++) {
                while (worker.zk.exists("/start" + i + "w" + k, false) == null) {
            	    // worker.zk.exists("/w" + previousWorkerIdToListen, true, worker, null);
		            System.out.println("Worker " + workerId + " Waiting for worker " + i + " to finish epoch" + k);
		            Thread.sleep(1000);
		        }
            }
        }
    }

    private List<Double> convertByteToDouble(byte[] data) {
        List<Double> list = new ArrayList<>();
        for (int j = 0; j < data.length / 8; j++) {
            list.add(ByteBuffer.wrap(data, 8 * j, 8).getDouble());
        }
        return list;
    }

    private void writeGradToZnode(int statusCode) throws KeeperException, InterruptedException {
	System.out.println("worker" + this.id + " writes grad to /w" + this.id + " with status " + statusCode + " in epoch " + this.curr_iter); 
        byte[] vector = new byte[this.grads.size() * 8 + 1];
        for (int i = 0; i < this.grads.size(); i++) {
            byte[] byteRep = ByteBuffer.allocate(8).putDouble(grads.get(i)).array();
            for (int j = 0; j < 8; j++)
                vector[8 * i + j] = byteRep[j];
        }
        //byte[] byteRep = ByteBuffer.allocate(8).putInt(statusCode).array();
        //for (int j = 8; j > 0; j--)
        //    vector[vector.length - j] = byteRep[8 - j];
	    vector[vector.length - 1] = (byte)statusCode;
        this.zk.setData("/w" + this.id, vector, -1);
    }

    private int getWorkerIdToListen() {
        int previousWorkerIdToListen;
        if (this.id > 0) {
            previousWorkerIdToListen = this.id - 1;
        }
        else {
            previousWorkerIdToListen = this.numWorker - 1;
        }
        return previousWorkerIdToListen;
    }

    private void writeGradToFile() {
	    System.out.println("Enter writeGradToFile() | worker" + this.id + " | iteration: " + this.curr_iter); 
        // This condition means all the data needs to get the updated all the grad.
        try {
            FileWriter fw = new FileWriter(new File("grads"+this.id+".txt"), false);
            for (int j = 0; j < this.grads.size(); j++) {
                fw.write(this.grads.get(j)/this.numWorker + "\n");
            }
            fw.close();

            ProcessBuilder pb = new ProcessBuilder("python", "update_params.py", "" + this.id);
            Process process = pb.start();
            if (process.waitFor() != 0)
                return;

            String path = "/start" + this.id + "w" + this.curr_iter;
            if (this.zk.exists(path, false) == null)
                this.zk.create(path, null, OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void process(WatchedEvent event) {
	    System.out.println("Enter process() | worker" + this.id + " | event path :" + event.toString());
        if (event.getPath() == null)
            return;
        int previousWorkerIdToListen = this.getWorkerIdToListen();
        this.zk.exists("/w" + previousWorkerIdToListen, true, this, null);
        try {
            byte[] data = this.zk.getData(event.getPath(), false, this.zk.exists(event.getPath(), false));

            if (data[data.length - 1] == 0 && this.id == 1) {
                
                List<Double> previousGrad = this.convertByteToDouble(data);
                this.previousGrad = previousGrad;
                if (this.grads == null) {
                    System.out.println("Worker " + this.id + " recieves update from Worker " + previousWorkerIdToListen + ", but waiting for himslef.");
                }
                else {
                    for (int i = 0; i < previousGrad.size(); i++) {
                        this.grads.set(i, this.grads.get(i) + previousGrad.get(i));
                    }
                    this.writeGradToZnode(1);
                }

            }
            else if (data[data.length - 1] == 1) {
                if (this.id == 0) {
                    List<Double> previousGrad = this.convertByteToDouble(data);
                    this.grads = previousGrad;
                    this.writeGradToZnode(2);
                    this.writeGradToFile();
                }
                else {
                    List<Double> previousGrad = this.convertByteToDouble(data);
                    this.previousGrad = previousGrad;
                    if (this.grads != null) {
                        for (int i = 0; i < previousGrad.size(); i++) {
                            this.grads.set(i, this.grads.get(i) + previousGrad.get(i));
                        }
                        this.writeGradToZnode(1);
    
                        if (this.id == this.numWorker - 1) {
                            this.writeGradToFile();
                        }
                    }
                    else {
                        System.out.println("Worker " + this.id + " recieves update from Worker " + previousWorkerIdToListen + ", but waiting for himslef.");
                    }

                }
            }
            else if(this.id > 0 && this.id < this.numWorker -1 ) {
                List<Double> previousGrad = this.convertByteToDouble(data);
                this.grads = previousGrad;
                this.writeGradToZnode(2);
                this.writeGradToFile();
            }

        }
        catch(Exception e) {
            e.printStackTrace();
        }
    }

    public void processResult(int rc, String path, Object ctx, Stat stat) {
        if (rc != Code.OK.intValue()) {
            System.out.println("Something's wrong: " + path);
        }
    }
}
