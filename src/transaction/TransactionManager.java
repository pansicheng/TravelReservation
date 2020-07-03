package transaction;

import java.rmi.*;

/**
 * Interface for the Transaction Manager of the Distributed Travel Reservation
 * System.
 * <p>
 * Unlike WorkflowController.java, you are supposed to make changes to this
 * file.
 */

public interface TransactionManager extends Remote {
    public boolean dieNow() throws RemoteException;

    /** The RMI name a TransactionManager binds to. */
    public static final String RMIName = "TM";

    public int start() throws RemoteException;

    public boolean commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException;

    public void abort(int xid) throws RemoteException, InvalidTransactionException;

    public void enlist(int xid, String component)
            throws RemoteException, InvalidTransactionException, TransactionAbortedException;

    public boolean reconnect() throws RemoteException;

    /**
     * Sets a flag so that the TM fails after it has received "prepared" messages
     * from all RMs, but before it can log "committed".
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @return true on success, false on failure.
     */
    public boolean dieTMBeforeCommit() throws RemoteException;

    /**
     * Sets a flag so that the TM fails right after it logs "committed".
     * <p>
     * This method is used for testing and is not part of a transaction.
     *
     * @return true on success, false on failure.
     */
    public boolean dieTMAfterCommit() throws RemoteException;
}
