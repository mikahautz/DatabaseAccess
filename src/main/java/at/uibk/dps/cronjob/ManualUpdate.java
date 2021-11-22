package at.uibk.dps.cronjob;

import at.uibk.dps.databases.MariaDBAccess;
import at.uibk.dps.databases.MongoDBAccess;

/**
 * Class to manually run the update of the metadata database.
 */
public class ManualUpdate {
    public static void main(String[] args) {
        MariaDBAccess.resetCounters();
        MariaDBAccess.setPrint(false);
        MongoDBAccess.findNewEntries().forEach(MariaDBAccess.updateMD);
    }
}
