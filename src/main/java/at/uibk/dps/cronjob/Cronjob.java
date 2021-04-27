package at.uibk.dps.cronjob;

import at.uibk.dps.databases.MariaDBAccess;
import at.uibk.dps.databases.MongoDBAccess;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimerTask;

public class Cronjob extends TimerTask {

    /**
     * Updates all undone logs in the metadata DB and updates its 'done'-field.
     */
    @Override
    public void run() {
        MariaDBAccess.resetCounters();
        MongoDBAccess.findNewEntries().forEach(MariaDBAccess.updateMD);

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        String completed = "# Updating metadata DB completed at " + dtf.format(now);
        String info = "# Updated: " + MariaDBAccess.getUpdated() + ", Skipped: " + MariaDBAccess.getSkipped();

        System.out.println("#############################################################");
        System.out.printf("%-60s#\n", completed);
        System.out.printf("%-60s#\n", info);
        System.out.println("#############################################################");
    }
}
