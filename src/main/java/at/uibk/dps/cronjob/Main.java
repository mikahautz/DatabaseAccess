package at.uibk.dps.cronjob;

import java.util.Timer;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) {
        Timer t = new Timer();
        Cronjob cronjob = new Cronjob();
        // run the update every hour
        t.scheduleAtFixedRate(cronjob, 0, TimeUnit.HOURS.toMillis(1));
    }
}
