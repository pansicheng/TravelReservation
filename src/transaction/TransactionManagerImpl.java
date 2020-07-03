package transaction;

import java.rmi.*;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Transaction Manager for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the TM
 */

public class TransactionManagerImpl extends java.rmi.server.UnicastRemoteObject implements TransactionManager {

    private static final long serialVersionUID = 1L;
    protected ResourceManager rmFlights = null;
    protected ResourceManager rmRooms = null;
    protected ResourceManager rmCars = null;
    protected ResourceManager rmCustomers = null;

    protected int xidCounter;
    private HashMap<Integer, HashSet<String>> activeTransactions;

    public static void main(String args[]) {
        System.setSecurityManager(new RMISecurityManager());

        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            TransactionManagerImpl obj = new TransactionManagerImpl();
            Naming.rebind(rmiPort + TransactionManager.RMIName, obj);
            System.out.println("TM bound");
        } catch (Exception e) {
            System.err.println("TM not bound:" + e);
            System.exit(1);
        }
    }

    public TransactionManagerImpl() throws RemoteException {
        activeTransactions = new HashMap<Integer, HashSet<String>>();
        xidCounter = 0;
    }

    public boolean dieNow() throws RemoteException {
        System.exit(1);
        return true; // We won't ever get here since we exited above;
                     // but we still need it to please the compiler.
    }

    public boolean reconnect() throws RemoteException {
        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            rmFlights = (ResourceManager) Naming.lookup(rmiPort + ResourceManager.RMINameFlights);
            System.out.println(RMIName + " bound to " + ResourceManager.RMINameFlights);
            rmRooms = (ResourceManager) Naming.lookup(rmiPort + ResourceManager.RMINameRooms);
            System.out.println(RMIName + " bound to " + ResourceManager.RMINameRooms);
            rmCars = (ResourceManager) Naming.lookup(rmiPort + ResourceManager.RMINameCars);
            System.out.println(RMIName + " bound to " + ResourceManager.RMINameCars);
            rmCustomers = (ResourceManager) Naming.lookup(rmiPort + ResourceManager.RMINameCustomers);
            System.out.println(RMIName + " bound to " + ResourceManager.RMINameCustomers);
            return true;
        } catch (Exception e) {
            System.err.println(RMIName + " cannot bind to some component: " + e);
            return false;
        }
    }

    public int start() throws RemoteException {
        System.out.println("TM start");
        if (rmFlights == null || rmCars == null || rmRooms == null || rmCustomers == null)
            while (!reconnect()) {
                // would be better to sleep a while
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }

        xidCounter++;
        activeTransactions.put(xidCounter, new HashSet<String>());
        return xidCounter;
    }

    public boolean commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        checkXid(xid);
        for (String rmiName : activeTransactions.get(xid))
            try {
                System.out.println("prepare " + rmiName + " #" + xid);
                switch (rmiName) {
                    case ResourceManager.RMINameFlights:
                        rmFlights.prepare(xid);
                        break;
                    case ResourceManager.RMINameRooms:
                        rmRooms.prepare(xid);
                        break;
                    case ResourceManager.RMINameCars:
                        rmCars.prepare(xid);
                        break;
                    case ResourceManager.RMINameCustomers:
                        rmCustomers.prepare(xid);
                        break;
                }
            } catch (RemoteException e) {
                System.out.println(rmiName + " prepare RemoteException: " + e.getMessage());
                abort(xid);
                throw new TransactionAbortedException(xid, e.getMessage());
            } catch (TransactionAbortedException e) {
                System.out.println(rmiName + " prepare TransactionAbortedException: " + e.getMessage());
                abort(xid);
                throw new TransactionAbortedException(xid, e.getMessage());
            } catch (InvalidTransactionException e) {
                System.out.println(rmiName + " prepare InvalidTransactionException: " + e.getMessage());
                abort(xid);
                throw new InvalidTransactionException(xid, e.getMessage());
            } catch (Exception e) {
                System.out.println(rmiName + " prepare exception: " + e.getMessage());
                abort(xid);
                throw new TransactionAbortedException(xid, e.getMessage());
            }

        if (flagDieTMBeforeCommit)
            dieNow();

        for (String rmiName : activeTransactions.get(xid))
            try {
                System.out.println("commit " + rmiName + " #" + xid);
                switch (rmiName) {
                    case ResourceManager.RMINameFlights:
                        rmFlights.commit(xid);
                        break;
                    case ResourceManager.RMINameRooms:
                        rmRooms.commit(xid);
                        break;
                    case ResourceManager.RMINameCars:
                        rmCars.commit(xid);
                        break;
                    case ResourceManager.RMINameCustomers:
                        rmCustomers.commit(xid);
                        break;
                }
            } catch (TransactionAbortedException e) {
                abort(xid);
                throw new TransactionAbortedException(xid, e.getMessage());
            } catch (InvalidTransactionException e) {
                abort(xid);
                throw new InvalidTransactionException(xid, e.getMessage());
            } catch (Exception e) {
                System.out.println(rmiName + " commit exception: " + e.getMessage());
            }

        if (flagDieTMAfterCommit)
            dieNow();

        for (String rmiName : activeTransactions.get(xid))
            try {
                System.out.println("unlock " + rmiName + " #" + xid);
                switch (rmiName) {
                    case ResourceManager.RMINameFlights:
                        rmFlights.lm_unlockAll(xid);
                        break;
                    case ResourceManager.RMINameRooms:
                        rmRooms.lm_unlockAll(xid);
                        break;
                    case ResourceManager.RMINameCars:
                        rmCars.lm_unlockAll(xid);
                        break;
                    case ResourceManager.RMINameCustomers:
                        rmCustomers.lm_unlockAll(xid);
                        break;
                }
            } catch (Exception e) {
                System.out.println(rmiName + " unlock exception " + e.getMessage());
            }

        activeTransactions.remove(xid);
        return true;
    }

    public void abort(int xid) throws RemoteException, InvalidTransactionException {
        for (String rmiName : activeTransactions.get(xid)) {
            System.out.println("#" + xid + " abort: " + rmiName);
            try {
                switch (rmiName) {
                    case ResourceManager.RMINameFlights:
                        rmFlights.abort(xid);
                        break;
                    case ResourceManager.RMINameRooms:
                        rmRooms.abort(xid);
                        break;
                    case ResourceManager.RMINameCars:
                        rmCars.abort(xid);
                        break;
                    case ResourceManager.RMINameCustomers:
                        rmCustomers.abort(xid);
                        break;
                    default:
                        throw new InvalidTransactionException(xid, "InvalidTransactionException TM abort");
                }
            } catch (Exception e) {
                System.out.println("abort exception: " + e.getMessage());
            }
        }
        activeTransactions.remove(xid);
        return;
    }

    public void enlist(int xid, String component)
            throws RemoteException, InvalidTransactionException, TransactionAbortedException {
        checkXid(xid);
        HashSet<String> rmiNames = activeTransactions.get(xid);
        rmiNames.add(component);
        activeTransactions.put(xid, rmiNames);
    }

    public void checkXid(int xid) throws RemoteException, InvalidTransactionException, TransactionAbortedException {
        if (activeTransactions.size() == 0)
            throw new TransactionAbortedException(xid, "TransactionAbortedException");
        else if (!activeTransactions.containsKey(xid))
            throw new InvalidTransactionException(xid, "InvalidTransactionException");
    }

    private boolean flagDieTMBeforeCommit = false;
    private boolean flagDieTMAfterCommit = false;

    public boolean dieTMBeforeCommit() throws RemoteException {
        flagDieTMBeforeCommit = true;
        return true;
    }

    public boolean dieTMAfterCommit() throws RemoteException {
        flagDieTMAfterCommit = true;
        return true;
    }
}
