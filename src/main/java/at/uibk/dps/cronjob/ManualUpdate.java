package at.uibk.dps.cronjob;

/**
 * Class to manually run the update of the metadata database.
 */
public class ManualUpdate {
    public static void main(String[] args) {
        Cronjob cronjob = new Cronjob();
        cronjob.run();
    }
}
