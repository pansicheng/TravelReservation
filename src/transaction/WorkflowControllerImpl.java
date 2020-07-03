package transaction;

import java.rmi.*;
import java.util.*;

import transaction.tables.ResvPair;
import transaction.tables.ReservationsTable;

/**
 * Workflow Controller for the Distributed Travel Reservation System.
 * 
 * Description: toy implementation of the WC. In the real implementation, the WC
 * should forward calls to either RM or TM, instead of doing the things itself.
 */

public class WorkflowControllerImpl extends java.rmi.server.UnicastRemoteObject implements WorkflowController {
    private static final long serialVersionUID = 1L;

    protected int xidCounter;

    protected ResourceManager rmFlights = null;
    protected ResourceManager rmRooms = null;
    protected ResourceManager rmCars = null;
    protected ResourceManager rmCustomers = null;
    protected TransactionManager tm = null;

    public static void main(String args[]) {
        System.setSecurityManager(new RMISecurityManager());

        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            WorkflowControllerImpl obj = new WorkflowControllerImpl();
            Naming.rebind(rmiPort + WorkflowController.RMIName, obj);
            System.out.println("WC bound");
        } catch (Exception e) {
            System.err.println("WC not bound:" + e);
            System.exit(1);
        }
    }

    public WorkflowControllerImpl() throws RemoteException {
        while (!reconnect()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // TRANSACTION INTERFACE
    public int start() throws RemoteException {
        System.out.println("WC start");
        int xid = tm.start();
        return xid;
    }

    public boolean commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        System.out.println("Committing");
        return tm.commit(xid);
    }

    public void abort(int xid) throws RemoteException, InvalidTransactionException {
        tm.abort(xid);
        return;
    }

    // ADMINISTRATIVE INTERFACE
    public boolean addFlight(int xid, String flightNum, int numSeats, int price)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmFlights.addFlight(xid, flightNum, numSeats, price);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public boolean deleteFlight(int xid, String flightNum)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmFlights.deleteFlight(xid, flightNum);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public boolean addRooms(int xid, String location, int numRooms, int price)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmRooms.addRooms(xid, location, numRooms, price);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public boolean deleteRooms(int xid, String location, int numRooms)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmRooms.deleteRooms(xid, location, numRooms);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public boolean addCars(int xid, String location, int numCars, int price)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmCars.addCars(xid, location, numCars, price);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public boolean deleteCars(int xid, String location, int numCars)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmCars.deleteCars(xid, location, numCars);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public boolean newCustomer(int xid, String custName)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmCustomers.newCustomer(xid, custName);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public boolean deleteCustomer(int xid, String custName)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmCustomers.deleteCustomer(xid, custName);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    // QUERY INTERFACE
    public int queryFlight(int xid, String flightNum)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmFlights.queryFlight(xid, flightNum);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public int queryFlightPrice(int xid, String flightNum)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmFlights.queryFlightPrice(xid, flightNum);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public int queryRooms(int xid, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmRooms.queryRooms(xid, location);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public int queryRoomsPrice(int xid, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmRooms.queryRoomsPrice(xid, location);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public int queryCars(int xid, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmCars.queryCars(xid, location);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public int queryCarsPrice(int xid, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            return rmCars.queryCarsPrice(xid, location);
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public int queryCustomerBill(int xid, String custName)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            int bill = 0;
            for (ResvPair resvPair : rmCustomers.queryCustomerResv(xid, custName)) {
                switch (resvPair.getResvType()) {
                    case ReservationsTable.resvTypeFlight:
                        bill += queryFlightPrice(xid, resvPair.getResvKey());
                        break;
                    case ReservationsTable.resvTypeHotelRoom:
                        bill += queryRoomsPrice(xid, resvPair.getResvKey());
                        break;
                    case ReservationsTable.resvTypeCar:
                        bill += queryCarsPrice(xid, resvPair.getResvKey());
                        break;
                }
            }
            return bill;
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    // RESERVATION INTERFACE
    public boolean reserveFlight(int xid, String custName, String flightNum)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            if (queryFlight(xid, flightNum) > 0) {
                return rmFlights.reserveFlight(xid, custName, flightNum)
                        && rmCustomers.reserveCustomer(xid, custName, ReservationsTable.resvTypeFlight, flightNum);
            } else
                return false;
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public boolean reserveCar(int xid, String custName, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            if (queryCars(xid, location) > 0) {
                return rmCars.reserveCar(xid, custName, location)
                        && rmCustomers.reserveCustomer(xid, custName, ReservationsTable.resvTypeCar, location);
            } else
                return false;
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public boolean reserveRoom(int xid, String custName, String location)
            throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            if (queryRooms(xid, location) > 0) {
                return rmRooms.reserveRoom(xid, custName, location)
                        && rmCustomers.reserveCustomer(xid, custName, ReservationsTable.resvTypeHotelRoom, location);
            } else
                return false;
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    public boolean reserveItinerary(int xid, String custName, List flightNumList, String location, boolean needCar,
            boolean needRoom) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
        try {
            if (needCar && queryCars(xid, location) <= 0)
                return false;
            if (needRoom && queryRooms(xid, location) <= 0)
                return false;
            for (Object object : flightNumList) {
                String flightNum = (String) object;
                if (queryFlight(xid, flightNum) <= 0)
                    return false;
            }

            for (Object object : flightNumList) {
                String flightNum = (String) object;
                if (!reserveFlight(xid, custName, flightNum))
                    return false;
            }
            if (needCar)
                if (!reserveCar(xid, custName, location))
                    return false;
            if (needRoom)
                if (!reserveRoom(xid, custName, location))
                    return false;
            return true;
        } catch (RemoteException e) {
            abort(xid);
            throw new RemoteException();
        } catch (TransactionAbortedException e) {
            abort(xid);
            throw new TransactionAbortedException(xid, e.getMessage());
        } catch (InvalidTransactionException e) {
            throw new InvalidTransactionException(xid, e.getMessage());
        }
    }

    // TECHNICAL/TESTING INTERFACE
    public boolean reconnect() throws RemoteException {
        String rmiPort = System.getProperty("rmiPort");
        if (rmiPort == null) {
            rmiPort = "";
        } else if (!rmiPort.equals("")) {
            rmiPort = "//:" + rmiPort + "/";
        }

        try {
            rmFlights = (ResourceManager) Naming.lookup(rmiPort + ResourceManager.RMINameFlights);
            System.out.println("WC bound to RMFlights");
            rmRooms = (ResourceManager) Naming.lookup(rmiPort + ResourceManager.RMINameRooms);
            System.out.println("WC bound to RMRooms");
            rmCars = (ResourceManager) Naming.lookup(rmiPort + ResourceManager.RMINameCars);
            System.out.println("WC bound to RMCars");
            rmCustomers = (ResourceManager) Naming.lookup(rmiPort + ResourceManager.RMINameCustomers);
            System.out.println("WC bound to RMCustomers");
            tm = (TransactionManager) Naming.lookup(rmiPort + TransactionManager.RMIName);
            System.out.println("WC bound to TM");
        } catch (Exception e) {
            System.err.println("WC cannot bind to some component:" + e);
            return false;
        }

        try {
            if (rmFlights.reconnect() && rmRooms.reconnect() && rmCars.reconnect() && rmCustomers.reconnect()
                    && tm.reconnect()) {
                return true;
            }
        } catch (Exception e) {
            System.err.println("Some RM cannot reconnect:" + e);
            return false;
        }

        return false;
    }

    public boolean dieNow(String who) throws RemoteException {
        if (who.equals(TransactionManager.RMIName) || who.equals("ALL")) {
            try {
                tm.dieNow();
            } catch (RemoteException e) {
            }
        }
        if (who.equals(ResourceManager.RMINameFlights) || who.equals("ALL")) {
            try {
                rmFlights.dieNow();
            } catch (RemoteException e) {
            }
        }
        if (who.equals(ResourceManager.RMINameRooms) || who.equals("ALL")) {
            try {
                rmRooms.dieNow();
            } catch (RemoteException e) {
            }
        }
        if (who.equals(ResourceManager.RMINameCars) || who.equals("ALL")) {
            try {
                rmCars.dieNow();
            } catch (RemoteException e) {
            }
        }
        if (who.equals(ResourceManager.RMINameCustomers) || who.equals("ALL")) {
            try {
                rmCustomers.dieNow();
            } catch (RemoteException e) {
            }
        }
        if (who.equals(WorkflowController.RMIName) || who.equals("ALL")) {
            System.exit(1);
        }
        return true;
    }

    public boolean dieRMAfterEnlist(String who) throws RemoteException {
        switch (who) {
            case ResourceManager.RMINameFlights:
                rmFlights.dieRMAfterEnlist();
                break;
            case ResourceManager.RMINameRooms:
                rmRooms.dieRMAfterEnlist();
                break;
            case ResourceManager.RMINameCars:
                rmCars.dieRMAfterEnlist();
                break;
            case ResourceManager.RMINameCustomers:
                rmCustomers.dieRMAfterEnlist();
                break;
            default:
                return false;
        }
        return true;
    }

    public boolean dieRMBeforePrepare(String who) throws RemoteException {
        switch (who) {
            case ResourceManager.RMINameFlights:
                rmFlights.dieRMBeforePrepare();
                break;
            case ResourceManager.RMINameRooms:
                rmRooms.dieRMBeforePrepare();
                break;
            case ResourceManager.RMINameCars:
                rmCars.dieRMBeforePrepare();
                break;
            case ResourceManager.RMINameCustomers:
                rmCustomers.dieRMBeforePrepare();
                break;
            default:
                return false;
        }
        return true;
    }

    public boolean dieRMAfterPrepare(String who) throws RemoteException {
        switch (who) {
            case ResourceManager.RMINameFlights:
                rmFlights.dieRMAfterPrepare();
                break;
            case ResourceManager.RMINameRooms:
                rmRooms.dieRMAfterPrepare();
                break;
            case ResourceManager.RMINameCars:
                rmCars.dieRMAfterPrepare();
                break;
            case ResourceManager.RMINameCustomers:
                rmCustomers.dieRMAfterPrepare();
                break;
            default:
                return false;
        }
        return true;
    }

    public boolean dieTMBeforeCommit() throws RemoteException {
        return tm.dieTMBeforeCommit();
    }

    public boolean dieTMAfterCommit() throws RemoteException {
        return tm.dieTMAfterCommit();
    }

    public boolean dieRMBeforeCommit(String who) throws RemoteException {
        switch (who) {
            case ResourceManager.RMINameFlights:
                rmFlights.dieRMBeforeCommit();
                break;
            case ResourceManager.RMINameRooms:
                rmRooms.dieRMBeforeCommit();
                break;
            case ResourceManager.RMINameCars:
                rmCars.dieRMBeforeCommit();
                break;
            case ResourceManager.RMINameCustomers:
                rmCustomers.dieRMBeforeCommit();
                break;
            default:
                return false;
        }
        return true;
    }

    public boolean dieRMBeforeAbort(String who) throws RemoteException {
        switch (who) {
            case ResourceManager.RMINameFlights:
                rmFlights.dieRMBeforeAbort();
                break;
            case ResourceManager.RMINameRooms:
                rmRooms.dieRMBeforeAbort();
                break;
            case ResourceManager.RMINameCars:
                rmCars.dieRMBeforeAbort();
                break;
            case ResourceManager.RMINameCustomers:
                rmCustomers.dieRMBeforeAbort();
                break;
            default:
                return false;
        }
        return true;
    }
}
