package raptor.server;

public class HeartbeatThread extends Thread {
    private static HeartbeatThread THREAD = null;
    private long timeToDie = 0;
    private boolean stopCountdown = false;

    public synchronized static HeartbeatThread getHeartbeatThread() {
        if (THREAD != null) {
            return THREAD;
        } else {
            THREAD = new HeartbeatThread();
            THREAD.start();
            return THREAD;
        }
    }

    synchronized static void resetHeartbeatThread() {
        THREAD = null;
    }

    public void stopCountdown() {
        synchronized (this) {
            this.stopCountdown = true;
            this.notify();
        }
    }

    public HeartbeatThread() {
        super();
        timeToDie = System.currentTimeMillis() + 250;
    }

    public void run() {
        synchronized (this) {
            long ttl = timeToDie - System.currentTimeMillis();
            while (ttl > 0) {
                try {
                    this.wait(ttl);
                    ttl = timeToDie - System.currentTimeMillis();
                }
                catch (InterruptedException e) {
                    System.exit(0);
                }
            }
            if (!stopCountdown) {
                System.exit(0);
            } else {
                resetHeartbeatThread();
            }
        }
    }
}
