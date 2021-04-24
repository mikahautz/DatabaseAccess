package at.uibk.dps.cronjob;

import java.util.Timer;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        Timer t = new Timer();
        Scheduler scheduler = new Scheduler();
        // run the update every hour
        t.scheduleAtFixedRate(scheduler, 0, TimeUnit.HOURS.toMillis(1));
    }

    /**
     * Method to manually run the update of the metadata database.
     */
    public static void updateMetadata() {
        Scheduler scheduler = new Scheduler();
        scheduler.run();
    }
}
