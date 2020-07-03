package transaction;

import lockmgr.*;
import transaction.tables.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.rmi.*;
import java.util.HashMap;
import java.util.ArrayList;

class TableModified implements Serializable {

    private static final long serialVersionUID = 1L;
    private String tableType;
    private String tableKey;

    public TableModified(String tableType, String tableKey) {
        this.tableType = tableType;
        this.tableKey = tableKey;
    }

    public String getTableType() {
        return tableType;
    }

    public String getTableKey() {
        return tableKey;
    }

}

/**
 * Resource Manager for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the RM
 */

public class ResourceManagerImpl extends java.rmi.server.UnicastRemoteObject implements ResourceManager {

    protected String myRMIName = null; // Used to distinguish this RM from other RMs
    protected TransactionManager tm = null;

    private static final long serialVersionUID = 1L;

    private static final String DATA_DIR = "data";

    private static final String KeyFlight = "FLIGHTS";
    private static final String KeyHotel = "HOTELS";
    private static final String KeyCar = "CARS";
    private static final String KeyReservation = "RESERVATIONS";

    // shadowing
    // non-active
    private static final String FLIGHTS_DB = DATA_DIR + "/" + KeyFlight + ".db";
    private static final String HOTELS_DB = DATA_DIR + "/" + KeyHotel + ".db";
    private static final String CARS_DB = DATA_DIR + "/" + KeyCar + ".db";
    private static final String RESERVATIONS_DB = DATA_DIR + "/" + KeyReservation + ".db";

    private FlightsTable flightsTable;
    private HotelsTable hotelsTable;
    private CarsTable carsTable;
    private ReservationsTable reservationsTable;

    // active
    private static final String A_FLIGHTS_DB = DATA_DIR + "/" + KeyFlight + ".adb";
    private static final String A_HOTELS_DB = DATA_DIR + "/" + KeyHotel + ".adb";
    private static final String A_CARS_DB = DATA_DIR + "/" + KeyCar + ".adb";
    private static final String A_RESERVATIONS_DB = DATA_DIR + "/" + KeyReservation + ".adb";

    private FlightsTable aFlightsTable;
    private HotelsTable aHotelsTable;
    private CarsTable aCarsTable;
    private ReservationsTable aReservationsTable;

    // prepare
    private static final String P_FLIGHTS_DB = DATA_DIR + "/" + KeyFlight + ".pdb";
    private static final String P_HOTELS_DB = DATA_DIR + "/" + KeyHotel + ".pdb";
    private static final String P_CARS_DB = DATA_DIR + "/" + KeyCar + ".pdb";
    private static final String P_RESERVATIONS_DB = DATA_DIR + "/" + KeyReservation + ".pdb";

    private static final String P_FLIGHTS_RECOVER = DATA_DIR + "/" + KeyFlight + ".rcv";
    private static final String P_HOTELS_RECOVER = DATA_DIR + "/" + KeyHotel + ".rcv";
    private static final String P_CARS_RECOVER = DATA_DIR + "/" + KeyCar + ".rcv";
    private static final String P_RESERVATIONS_RECOVER = DATA_DIR + "/" + KeyReservation + ".rcv";

    private static LockManager lm;
    private HashMap<Integer, ArrayList<TableModified>> activeTransactions;
    private HashMap<Integer, Boolean> preparedTransactions;

    public static void main(String args[]) {
        System.setSecurityManager(new RMISecurityManager());

        String rmiName = System.getProperty("rmiName");
        if (rmiName == null || rmiName.equals("")) {
            System.err.println("No RMI name given");
            System.exit(1);
        }

        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            ResourceManagerImpl obj = new ResourceManagerImpl(rmiName);
            Naming.rebind(rmiPort + rmiName, obj);
            System.out.println(rmiName + " bound");
        } catch (Exception e) {
            System.err.println(rmiName + " not bound:" + e);
            System.exit(1);
        }
    }

    public ResourceManagerImpl(String rmiName) throws RemoteException {
        myRMIName = rmiName;

        while (!reconnect()) {
            // would be better to sleep a while
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        checkDataDir();
        recover();
        activeTransactions = new HashMap<Integer, ArrayList<TableModified>>();
        preparedTransactions = new HashMap<Integer, Boolean>();
        if (lm == null)
            lm = new LockManager();
    }

    public boolean reconnect() throws RemoteException {
        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            tm = (TransactionManager) Naming.lookup(rmiPort + TransactionManager.RMIName);
            System.out.println(myRMIName + " bound to " + TransactionManager.RMIName);
        } catch (Exception e) {
            System.err.println(myRMIName + " cannot bind to TM:" + e);
            return false;
        }

        return true;
    }

    private void checkDataDir() {
        File DataDir = new File(DATA_DIR);
        if (!DataDir.exists())
            try {
                DataDir.mkdir();
            } catch (Exception e) {
                e.printStackTrace();
            }
    }

    private void checkXid(int xid) throws RemoteException, InvalidTransactionException, TransactionAbortedException {
        if (activeTransactions.size() == 0) {
            throw new TransactionAbortedException(xid, "TransactionAbortedException");
        } else if (!activeTransactions.containsKey(xid))
            throw new InvalidTransactionException(xid, "InvalidTransactionException");
    }

    private boolean recover() {
        File file;
        if (myRMIName.equals(RMINameFlights)) {
            file = new File(P_FLIGHTS_RECOVER);
            if (file.exists()) {
                System.out.println(myRMIName + " P_FLIGHTS_RECOVER");
                file.delete();
                file = new File(P_FLIGHTS_DB);
                try {
                    if (file.exists()) {
                        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(P_FLIGHTS_DB));
                        aFlightsTable = (FlightsTable) ois.readObject();
                        ois.close();
                        store(FLIGHTS_DB, aFlightsTable);
                        store(A_FLIGHTS_DB, aFlightsTable);
                        file.delete();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
            file = new File(FLIGHTS_DB);
            try {
                if (file.exists()) {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(FLIGHTS_DB));
                    flightsTable = (FlightsTable) ois.readObject();
                    ois.close();
                } else
                    flightsTable = new FlightsTable();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            file = new File(A_FLIGHTS_DB);
            try {
                if (file.exists()) {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(A_FLIGHTS_DB));
                    aFlightsTable = (FlightsTable) ois.readObject();
                    ois.close();
                } else
                    aFlightsTable = new FlightsTable();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        } else if (myRMIName.equals(RMINameRooms)) {
            file = new File(P_HOTELS_RECOVER);
            if (file.exists()) {
                System.out.println(myRMIName + " P_HOTELS_RECOVER");
                file.delete();
                file = new File(P_HOTELS_DB);
                try {
                    if (file.exists()) {
                        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(P_HOTELS_DB));
                        aHotelsTable = (HotelsTable) ois.readObject();
                        ois.close();
                        store(HOTELS_DB, aHotelsTable);
                        store(A_HOTELS_DB, aHotelsTable);
                        file.delete();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
            file = new File(HOTELS_DB);
            try {
                if (file.exists()) {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(HOTELS_DB));
                    hotelsTable = (HotelsTable) ois.readObject();
                    ois.close();
                } else
                    hotelsTable = new HotelsTable();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            file = new File(A_HOTELS_DB);
            try {
                if (file.exists()) {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(A_HOTELS_DB));
                    aHotelsTable = (HotelsTable) ois.readObject();
                    ois.close();
                } else
                    aHotelsTable = new HotelsTable();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        } else if (myRMIName.equals(RMINameCars)) {
            file = new File(P_CARS_RECOVER);
            if (file.exists()) {
                System.out.println(myRMIName + " P_CARS_RECOVER");
                file.delete();
                file = new File(P_CARS_DB);
                try {
                    if (file.exists()) {
                        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(P_CARS_DB));
                        aCarsTable = (CarsTable) ois.readObject();
                        ois.close();
                        store(CARS_DB, aCarsTable);
                        store(A_CARS_DB, aCarsTable);
                        file.delete();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
            file = new File(CARS_DB);
            try {
                if (file.exists()) {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(CARS_DB));
                    carsTable = (CarsTable) ois.readObject();
                    ois.close();
                } else
                    carsTable = new CarsTable();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            file = new File(A_CARS_DB);
            try {
                if (file.exists()) {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(A_CARS_DB));
                    aCarsTable = (CarsTable) ois.readObject();
                    ois.close();
                } else
                    aCarsTable = new CarsTable();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        } else if (myRMIName.equals(RMINameCustomers)) {
            file = new File(P_RESERVATIONS_RECOVER);
            if (file.exists()) {
                System.out.println(myRMIName + " P_RESERVATIONS_RECOVER");
                file.delete();
                file = new File(P_RESERVATIONS_DB);
                try {
                    if (file.exists()) {
                        ObjectInputStream ois = new ObjectInputStream(new FileInputStream(P_RESERVATIONS_DB));
                        aReservationsTable = (ReservationsTable) ois.readObject();
                        ois.close();
                        store(RESERVATIONS_DB, aReservationsTable);
                        store(A_RESERVATIONS_DB, aReservationsTable);
                        file.delete();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
            }
            file = new File(RESERVATIONS_DB);
            try {
                if (file.exists()) {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(RESERVATIONS_DB));
                    reservationsTable = (ReservationsTable) ois.readObject();
                    ois.close();
                } else
                    reservationsTable = new ReservationsTable();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
            file = new File(A_RESERVATIONS_DB);
            try {
                if (file.exists()) {
                    ObjectInputStream ois = new ObjectInputStream(new FileInputStream(A_RESERVATIONS_DB));
                    aReservationsTable = (ReservationsTable) ois.readObject();
                    ois.close();
                } else
                    aReservationsTable = new ReservationsTable();
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    private void store(String path, Object object) {
        try {
            File file = new File(path);
            if (!file.exists()) {
                file.createNewFile();
            }
            FileOutputStream fos = new FileOutputStream(file, false);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(object);
            oos.close();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void delete(String path) {
        try {
            File file = new File(path);
            if (file.exists())
                file.delete();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private void undo(int xid) throws RemoteException, InvalidTransactionException {
        for (TableModified tableModified : activeTransactions.get(xid))
            switch (tableModified.getTableType()) {
                case KeyFlight:
                    if (flightsTable.containsKey(tableModified.getTableKey())) {
                        FlightsRow flightsRow = flightsTable.get(tableModified.getTableKey());
                        aFlightsTable.put(tableModified.getTableKey(), new FlightsRow(flightsRow.getFlightNum(),
                                flightsRow.getPrice(), flightsRow.getNumSeats(), flightsRow.getNumAvail()));
                    } else
                        aFlightsTable.deleteFlight(tableModified.getTableKey());
                    break;
                case KeyHotel:
                    if (hotelsTable.containsKey(tableModified.getTableKey())) {
                        HotelsRow hotelsRow = hotelsTable.get(tableModified.getTableKey());
                        aHotelsTable.put(tableModified.getTableKey(), new HotelsRow(hotelsRow.getLocation(),
                                hotelsRow.getPrice(), hotelsRow.getNumRooms(), hotelsRow.getNumAvail()));
                    } else
                        aHotelsTable.remove(tableModified.getTableKey());
                    break;
                case KeyCar:
                    if (carsTable.containsKey(tableModified.getTableKey())) {
                        CarsRow carsRow = carsTable.get(tableModified.getTableKey());
                        aCarsTable.put(tableModified.getTableKey(), new CarsRow(carsRow.getLocation(),
                                carsRow.getPrice(), carsRow.getNumCars(), carsRow.getNumAvail()));
                    } else
                        aCarsTable.remove(tableModified.getTableKey());
                    break;
                case KeyReservation:
                    if (reservationsTable.containsKey(tableModified.getTableKey()))
                        aReservationsTable.put(tableModified.getTableKey(),
                                reservationsTable.getClone(tableModified.getTableKey()));
                    else
                        aReservationsTable.deleteCustomer(tableModified.getTableKey());
                    break;
                default:
                    throw new InvalidTransactionException(xid,
                            "Merge invalid updates into the non-active database copy.");
            }
    }

    private void updateActiveTransactions(int xid, String Table, String TableKey) {
        ArrayList<TableModified> tableModifieds;
        if (activeTransactions.containsKey(xid))
            tableModifieds = activeTransactions.get(xid);
        else
            tableModifieds = new ArrayList<TableModified>();
        tableModifieds.add(new TableModified(Table, TableKey));
        activeTransactions.put(xid, tableModifieds);
    }

    private void tm_enlist(int xid, String rmiName)
            throws RemoteException, InvalidTransactionException, TransactionAbortedException {
        tm.enlist(xid, rmiName);
        if (!activeTransactions.containsKey(xid))
            activeTransactions.put(xid, new ArrayList<TableModified>());
        if (!preparedTransactions.containsKey(xid))
            preparedTransactions.put(xid, false);
        if (flagDieRMAfterEnlist)
            dieNow();
    }

    public boolean lm_unlockAll(int xid) throws RemoteException {
        System.out.println(myRMIName + " unlock #" + xid);
        return lm.unlockAll(xid);
    }

    // TRANSACTION INTERFACE
    public boolean prepare(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        if (flagDieRMBeforePrepare)
            dieNow();
        checkXid(xid);
        boolean MF = false, MH = false, MC = false, MR = false;
        for (TableModified tableModified : activeTransactions.get(xid))
            switch (tableModified.getTableType()) {
                case KeyFlight:
                    MF = true;
                    break;
                case KeyHotel:
                    MH = true;
                    break;
                case KeyCar:
                    MC = true;
                    break;
                case KeyReservation:
                    MR = true;
                    break;
                default:
                    throw new InvalidTransactionException(xid, "InvalidTransactionException");
            }
        if (myRMIName.equals(ResourceManager.RMINameFlights) && MF)
            store(P_FLIGHTS_DB, aFlightsTable);
        if (myRMIName.equals(ResourceManager.RMINameRooms) && MH)
            store(P_HOTELS_DB, aHotelsTable);
        if (myRMIName.equals(ResourceManager.RMINameCars) && MC)
            store(P_CARS_DB, aCarsTable);
        if (myRMIName.equals(ResourceManager.RMINameCustomers) && MR)
            store(P_RESERVATIONS_DB, aReservationsTable);
        System.out.println("prepare store " + myRMIName + " #" + xid + " P_DB");
        preparedTransactions.put(xid, true);
        if (flagDieRMAfterPrepare)
            dieNow();
        return true;
    }

    public boolean commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        System.out.println("Committing");

        switch (myRMIName) {
            case ResourceManager.RMINameFlights:
                store(P_FLIGHTS_RECOVER, null);
                break;
            case ResourceManager.RMINameRooms:
                store(P_HOTELS_RECOVER, null);
                break;
            case ResourceManager.RMINameCars:
                store(P_CARS_RECOVER, null);
                break;
            case ResourceManager.RMINameCustomers:
                store(P_RESERVATIONS_RECOVER, null);
                break;
        }

        checkXid(xid);
        System.out.println("prepared " + myRMIName + " #" + xid + ": " + preparedTransactions.get(xid));
        if (!preparedTransactions.get(xid))
            return false;
        if (flagDieRMBeforeCommit)
            dieNow();
        boolean MF = false, MH = false, MC = false, MR = false;
        for (TableModified tableModified : activeTransactions.get(xid))
            switch (tableModified.getTableType()) {
                case KeyFlight:
                    if (aFlightsTable.containsKey(tableModified.getTableKey())) {
                        FlightsRow flightsRow = aFlightsTable.get(tableModified.getTableKey());
                        flightsTable.put(tableModified.getTableKey(), new FlightsRow(flightsRow.getFlightNum(),
                                flightsRow.getPrice(), flightsRow.getNumSeats(), flightsRow.getNumAvail()));
                    } else
                        flightsTable.deleteFlight(tableModified.getTableKey());
                    MF = true;
                    break;
                case KeyHotel:
                    if (aHotelsTable.containsKey(tableModified.getTableKey())) {
                        HotelsRow hotelsRow = aHotelsTable.get(tableModified.getTableKey());
                        hotelsTable.put(tableModified.getTableKey(), new HotelsRow(hotelsRow.getLocation(),
                                hotelsRow.getPrice(), hotelsRow.getNumRooms(), hotelsRow.getNumAvail()));
                    } else
                        hotelsTable.remove(tableModified.getTableKey());
                    MH = true;
                    break;
                case KeyCar:
                    if (aCarsTable.containsKey(tableModified.getTableKey())) {
                        CarsRow carsRow = aCarsTable.get(tableModified.getTableKey());
                        carsTable.put(tableModified.getTableKey(), new CarsRow(carsRow.getLocation(),
                                carsRow.getPrice(), carsRow.getNumCars(), carsRow.getNumAvail()));
                    } else
                        carsTable.remove(tableModified.getTableKey());
                    MC = true;
                    break;
                case KeyReservation:
                    if (aReservationsTable.containsKey(tableModified.getTableKey()))
                        reservationsTable.put(tableModified.getTableKey(),
                                aReservationsTable.getClone(tableModified.getTableKey()));
                    else
                        reservationsTable.deleteCustomer(tableModified.getTableKey());
                    MR = true;
                    break;
                default:
                    throw new InvalidTransactionException(xid,
                            "Merge invalid updates into the non-active database copy.");
            }

        if (flagDieBeforePointerSwitch)
            dieNow();

        if (myRMIName.equals(ResourceManager.RMINameFlights) && MF) {
            store(FLIGHTS_DB, flightsTable);
            store(A_FLIGHTS_DB, aFlightsTable);
            delete(P_FLIGHTS_DB);
        }
        if (myRMIName.equals(ResourceManager.RMINameRooms) && MH) {
            store(HOTELS_DB, hotelsTable);
            store(A_HOTELS_DB, aHotelsTable);
            delete(P_HOTELS_DB);
        }
        if (myRMIName.equals(ResourceManager.RMINameCars) && MC) {
            store(CARS_DB, carsTable);
            store(A_CARS_DB, aCarsTable);
            delete(P_CARS_DB);
        }
        if (myRMIName.equals(ResourceManager.RMINameCustomers) && MR) {
            store(RESERVATIONS_DB, reservationsTable);
            store(A_RESERVATIONS_DB, aReservationsTable);
            delete(P_RESERVATIONS_DB);
        }

        switch (myRMIName) {
            case ResourceManager.RMINameFlights:
                delete(P_FLIGHTS_RECOVER);
                break;
            case ResourceManager.RMINameRooms:
                delete(P_HOTELS_RECOVER);
                break;
            case ResourceManager.RMINameCars:
                delete(P_CARS_RECOVER);
                break;
            case ResourceManager.RMINameCustomers:
                delete(P_RESERVATIONS_RECOVER);
                break;
        }

        if (flagDieAfterPointerSwitch)
            dieNow();

        activeTransactions.remove(xid);
        preparedTransactions.remove(xid);

        return true;
    }

    public void abort(int xid) throws RemoteException, InvalidTransactionException {
        if (activeTransactions.containsKey(xid)) {
            System.out.println(myRMIName + " abort #" + xid);
            if (flagDieRMBeforeAbort)
                dieNow();
            undo(xid);
        }
        activeTransactions.remove(xid);
        if (preparedTransactions.containsKey(xid) && preparedTransactions.get(xid)) {
            switch (myRMIName) {
                case ResourceManager.RMINameFlights:
                    delete(P_FLIGHTS_DB);
                    break;
                case ResourceManager.RMINameRooms:
                    delete(P_HOTELS_DB);
                    break;
                case ResourceManager.RMINameCars:
                    delete(P_CARS_DB);
                    break;
                case ResourceManager.RMINameCustomers:
                    delete(P_RESERVATIONS_DB);
                    break;
            }
            System.out.println("abort delete " + myRMIName + " #" + xid + " P_DB");
        }
        preparedTransactions.remove(xid);
        lm_unlockAll(xid);
    }

    // ADMINISTRATIVE INTERFACE
    public boolean addFlight(int xid, String flightNum, int numSeats, int price)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameFlights);
        try {
            if (lm.lock(xid, KeyFlight + flightNum, LockManager.WRITE)) {
                updateActiveTransactions(xid, KeyFlight, flightNum);
                return aFlightsTable.addFlight(flightNum, numSeats, price);
            } else
                return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public boolean deleteFlight(int xid, String flightNum)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameFlights);
        try {
            if (lm.lock(xid, KeyFlight + flightNum, LockManager.WRITE))
                if (aFlightsTable.containsKey(flightNum))
                    if (aFlightsTable.get(flightNum).getNumSeats() == aFlightsTable.get(flightNum).getNumAvail()) {
                        updateActiveTransactions(xid, KeyFlight, flightNum);
                        return aFlightsTable.deleteFlight(flightNum);
                    }
            return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public boolean addRooms(int xid, String location, int numRooms, int price)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameRooms);
        try {
            if (lm.lock(xid, KeyHotel + location, LockManager.WRITE)) {
                updateActiveTransactions(xid, KeyHotel, location);
                return aHotelsTable.addRooms(location, numRooms, price);
            } else
                return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public boolean deleteRooms(int xid, String location, int numRooms)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameRooms);
        try {
            if (lm.lock(xid, KeyHotel + location, LockManager.WRITE))
                if (aHotelsTable.containsKey(location))
                    if (aHotelsTable.get(location).getNumRooms() == aHotelsTable.get(location).getNumAvail()) {
                        updateActiveTransactions(xid, KeyHotel, location);
                        return aHotelsTable.deleteRooms(location, numRooms);
                    }
            return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public boolean addCars(int xid, String location, int numCars, int price)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameCars);
        try {
            if (lm.lock(xid, KeyCar + location, LockManager.WRITE)) {
                updateActiveTransactions(xid, KeyCar, location);
                return aCarsTable.addCars(location, numCars, price);
            } else
                return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public boolean deleteCars(int xid, String location, int numCars)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameCars);
        try {
            if (lm.lock(xid, KeyCar + location, LockManager.WRITE))
                if (aCarsTable.containsKey(location))
                    if (aCarsTable.get(location).getNumCars() == aCarsTable.get(location).getNumAvail()) {
                        updateActiveTransactions(xid, KeyCar, location);
                        return aCarsTable.deleteCars(location, numCars);
                    }
            return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public boolean newCustomer(int xid, String custName)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameCustomers);
        try {
            if (lm.lock(xid, KeyReservation + custName, LockManager.WRITE)) {
                updateActiveTransactions(xid, KeyReservation, custName);
                return aReservationsTable.newCustomer(custName);
            } else
                return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public boolean deleteCustomer(int xid, String custName)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameCustomers);
        try {
            if (lm.lock(xid, KeyReservation + custName, LockManager.WRITE)) {
                for (ResvPair resvPair : aReservationsTable.get(custName)) {
                    if (resvPair.getResvType() == ReservationsTable.resvTypeFlight) {
                        try {
                            if (lm.lock(xid, KeyFlight + resvPair.getResvKey(), LockManager.WRITE)) {
                                aFlightsTable.cancelFlight(resvPair.getResvKey());
                                updateActiveTransactions(xid, KeyFlight, resvPair.getResvKey());
                            } else
                                return false;
                        } catch (DeadlockException e) {
                            throw new TransactionAbortedException(xid, "Transaction aborted by deadlock issue");
                        }
                    } else if (resvPair.getResvType() == ReservationsTable.resvTypeHotelRoom) {
                        try {
                            if (lm.lock(xid, KeyHotel + resvPair.getResvKey(), LockManager.WRITE)) {
                                aHotelsTable.cancelRoom(resvPair.getResvKey());
                                updateActiveTransactions(xid, KeyHotel, resvPair.getResvKey());
                            } else
                                return false;
                        } catch (DeadlockException e) {
                            throw new TransactionAbortedException(xid, "Transaction aborted by deadlock issue");
                        }
                    } else if (resvPair.getResvType() == ReservationsTable.resvTypeCar) {
                        try {
                            if (lm.lock(xid, KeyCar + resvPair.getResvKey(), LockManager.WRITE)) {
                                aCarsTable.cancelCar(resvPair.getResvKey());
                                updateActiveTransactions(xid, KeyCar, resvPair.getResvKey());
                            } else
                                return false;
                        } catch (DeadlockException e) {
                            throw new TransactionAbortedException(xid, "Transaction aborted by deadlock issue");
                        }
                    } else
                        return false;
                }
                updateActiveTransactions(xid, KeyReservation, custName);
                return aReservationsTable.deleteCustomer(custName);
            } else
                return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    // QUERY INTERFACE
    public int queryFlight(int xid, String flightNum)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameFlights);
        try {
            if (lm.lock(xid, KeyFlight + flightNum, LockManager.READ))
                return aFlightsTable.queryFlight(flightNum);
            else
                return -1;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public int queryFlightPrice(int xid, String flightNum)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameFlights);
        try {
            if (lm.lock(xid, KeyFlight + flightNum, LockManager.READ))
                return aFlightsTable.queryFlightPrice(flightNum);
            else
                return -1;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public int queryRooms(int xid, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameRooms);
        try {
            if (lm.lock(xid, KeyHotel + location, LockManager.READ))
                return aHotelsTable.queryRooms(location);
            else
                return -1;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public int queryRoomsPrice(int xid, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameRooms);
        try {
            if (lm.lock(xid, KeyHotel + location, LockManager.READ))
                return aHotelsTable.queryRoomsPrice(location);
            else
                return -1;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public int queryCars(int xid, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameCars);
        try {
            if (lm.lock(xid, KeyCar + location, LockManager.READ))
                return aCarsTable.queryCars(location);
            else
                return -1;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public int queryCarsPrice(int xid, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameCars);
        try {
            if (lm.lock(xid, KeyCar + location, LockManager.READ))
                return aCarsTable.queryCarsPrice(location);
            else
                return -1;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public ArrayList<ResvPair> queryCustomerResv(int xid, String custName)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameCustomers);
        try {
            if (lm.lock(xid, KeyReservation + custName, LockManager.READ))
                if (aReservationsTable.containsKey(custName))
                    return aReservationsTable.get(custName);
            return new ArrayList<ResvPair>();
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    // RESERVATION INTERFACE
    public boolean reserveFlight(int xid, String custName, String flightNum)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameFlights);
        try {
            if (lm.lock(xid, KeyFlight + flightNum, LockManager.WRITE)) {
                updateActiveTransactions(xid, KeyFlight, flightNum);
                return aFlightsTable.reserveFlight(flightNum);
            }
            return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public boolean reserveCar(int xid, String custName, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameCars);
        try {
            if (lm.lock(xid, KeyCar + location, LockManager.WRITE)) {
                updateActiveTransactions(xid, KeyCar, location);
                return aCarsTable.reserveCar(location);
            }
            return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public boolean reserveRoom(int xid, String custName, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameRooms);
        try {
            if (lm.lock(xid, KeyHotel + location, LockManager.WRITE)) {
                updateActiveTransactions(xid, KeyHotel, location);
                return aHotelsTable.reserveRoom(location);
            }
            return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    public boolean reserveCustomer(int xid, String custName, int resvType, String resvKey)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        tm_enlist(xid, ResourceManager.RMINameCustomers);
        try {
            if (lm.lock(xid, KeyReservation + custName, LockManager.WRITE)) {
                updateActiveTransactions(xid, KeyReservation, custName);
                return aReservationsTable.addResvPair(custName, resvType, resvKey);
            }
            return false;
        } catch (DeadlockException e) {
            throw new TransactionAbortedException(xid, "DeadlockException");
        }
    }

    // TECHNICAL/TESTING INTERFACE
    public boolean shutdown() throws RemoteException {
        System.exit(0);
        return true;
    }

    public boolean dieNow() throws RemoteException {
        System.exit(1);
        return true; // We won't ever get here since we exited above;
                     // but we still need it to please the compiler.
    }

    private boolean flagDieBeforePointerSwitch = false;
    private boolean flagDieAfterPointerSwitch = false;

    private boolean flagDieRMAfterEnlist = false;

    private boolean flagDieRMBeforePrepare = false;
    private boolean flagDieRMAfterPrepare = false;
    private boolean flagDieRMBeforeCommit = false;
    private boolean flagDieRMBeforeAbort = false;

    public boolean dieBeforePointerSwitch() throws RemoteException {
        flagDieBeforePointerSwitch = true;
        return true;
    }

    public boolean dieAfterPointerSwitch() throws RemoteException {
        flagDieAfterPointerSwitch = true;
        return true;
    }

    public boolean dieRMAfterEnlist() throws RemoteException {
        flagDieRMAfterEnlist = true;
        return true;
    }

    public boolean dieRMBeforePrepare() throws RemoteException {
        flagDieRMBeforePrepare = true;
        return true;
    }

    public boolean dieRMAfterPrepare() throws RemoteException {
        flagDieRMAfterPrepare = true;
        return true;
    }

    public boolean dieRMBeforeCommit() throws RemoteException {
        flagDieRMBeforeCommit = true;
        return true;
    }

    public boolean dieRMBeforeAbort() throws RemoteException {
        flagDieRMBeforeAbort = true;
        return true;
    }
}
