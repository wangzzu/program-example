package com.matt.test.bookkeeper.ledger;

import java.io.IOException;
import java.util.Enumeration;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LedgerTest {

    private BookKeeper bkClient;

    private static final Logger logger = LoggerFactory.getLogger(LedgerTest.class);


    public static void main(String[] args) {
        LedgerTest ledgerTest = new LedgerTest(args[0]);
        int number = Integer.valueOf(args[1]);
        // create a ledger
        LedgerHandle ledgerHandle = ledgerTest.createLedgerSync("matt blog");
        long ledgerId = ledgerHandle.getId();
        // write 100 entries
        int num = 100;
        for (int i = 0; i < number; i++) {
            ledgerTest.addEntry(ledgerHandle, i + " matt");
        }
        // close the ledger
        try {
            ledgerHandle.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (BKException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        // open the ledger
        try {
            ledgerHandle = ledgerTest.getBkClient()
                .openLedger(ledgerId, BookKeeper.DigestType.MAC, "matt blog".getBytes());
        } catch (BKException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        // read the entries
        Enumeration<LedgerEntry> entries = ledgerTest.readEntry(ledgerHandle, 0, num - 1);
        while (entries.hasMoreElements()) {
            System.out.println("The entry: " + new String(entries.nextElement().getEntry()));
        }
        //close the ledger and the client
        try {
            ledgerHandle.close();
            ledgerTest.getBkClient().close();
        } catch (BKException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        System.out.println("LedgerTest run end.");
    }

    /**
     * get the bkClient
     */
    public BookKeeper getBkClient() {
        return bkClient;
    }

    /**
     * init the BookKeeper client
     */
    public LedgerTest(String zkAddr) {
        try {
//            /* first method */
//            String connectionString = zkAddr; // For a single-node, local ZooKeeper cluster
//            BookKeeper bkClient = new BookKeeper(connectionString);
            /* second method*/
            ClientConfiguration config = new ClientConfiguration();
            config.setZkServers(zkAddr);
            config.setAddEntryTimeout(2000);
            BookKeeper bkClient = new BookKeeper(config);
            this.bkClient = bkClient;
            logger.info("BookKeeper client init success.");
        } catch (InterruptedException | IOException | BKException e) {
            e.printStackTrace();
            throw new RuntimeException(
                "There is an exception throw while creating the BookKeeper client.");
        }
    }

    /**
     * create the ledger, default ensemble size is 3, write quorum size is 2, ack quorum size is 2
     *
     * @param pw password
     * @return LedgerHandle
     */
    public LedgerHandle createLedgerSync(String pw) {
        byte[] password = pw.getBytes();
        try {
            LedgerHandle handle = bkClient.createLedger(BookKeeper.DigestType.MAC, password);
            return handle;
        } catch (BKException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * create the ledger
     *
     * @param pw password
     */
    public void createLedgerAsync(String pw) {

        class LedgerCreationCallback implements AsyncCallback.CreateCallback {

            public void createComplete(int returnCode, LedgerHandle handle, Object ctx) {
                System.out.println("Ledger successfully created");
                logger.info("Ledger successfully created async.");
            }
        }

        bkClient.asyncCreateLedger(
            3,
            2,
            BookKeeper.DigestType.MAC,
            pw.getBytes(),
            new LedgerCreationCallback(),
            "some context"
        );
    }

    /**
     * add the entry to the ledger
     *
     * @param ledgerHandle the ledger
     * @param msg msg
     * @return entryId, if occur exception, return -1
     */
    public long addEntry(LedgerHandle ledgerHandle, String msg) {
        try {
            return ledgerHandle.addEntry(msg.getBytes());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
        return -1;
    }

    /**
     * read entry from startId to endId
     *
     * @param ledgerHandle the ledger
     * @param startId start entry id
     * @param endId end entry id
     * @return the entries, if occur exception, return null
     */
    public Enumeration<LedgerEntry> readEntry(LedgerHandle ledgerHandle, int startId, int endId) {
        try {
            return ledgerHandle.readEntries(startId, endId);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * read entry from 0 to the LAC
     *
     * @param ledgerHandle the ledger
     * @return the entries, if occur exception, return null
     */
    public Enumeration<LedgerEntry> readEntry(LedgerHandle ledgerHandle) {
        try {
            return ledgerHandle.readEntries(0, ledgerHandle.getLastAddConfirmed());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * read entry after the LastAddConfirmed range
     *
     * @param ledgerHandle the handle
     * @param lastEntryIdExpectedToRead the last entry id
     * @return the entries, if occur exception, return null
     */
    public Enumeration<LedgerEntry> readEntry(LedgerHandle ledgerHandle,
        long lastEntryIdExpectedToRead) {
        try {
            return ledgerHandle.readUnconfirmedEntries(0, lastEntryIdExpectedToRead);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (BKException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * delete the ledger
     *
     * @param ledgerId the ledger id
     * @return if occur exception, return false
     */
    public boolean deleteLedger(long ledgerId) {
        try {
            bkClient.deleteLedger(ledgerId);
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
}
