package at.uibk.dps.cronjob;

import at.uibk.dps.databases.MariaDBAccess;
import at.uibk.dps.databases.MongoDBAccess;

import java.util.TimerTask;

public class Cronjob extends TimerTask {

    /**
     * Updates all undone logs in the metadata DB and sets its 'done'-field to true.
     */
    @Override
    public void run() {
        MongoDBAccess.findNewEntries().forEach(MariaDBAccess.updateMD);
    }
}
