package transaction;

import java.rmi.*;
import java.util.ArrayList;

import transaction.tables.ResvPair;

/**
 * Interface for the Resource Manager of the Distributed Travel Reservation
 * System.
 * <p>
 * Unlike WorkflowController.java, you are supposed to make changes to this
 * file.
 */

public interface ResourceManager extends Remote {

        public boolean reconnect() throws RemoteException;

        /** The RMI names a ResourceManager binds to. */
        public static final String RMINameFlights = "RMFlights";
        public static final String RMINameRooms = "RMRooms";
        public static final String RMINameCars = "RMCars";
        public static final String RMINameCustomers = "RMCustomers";

        //////////
        // TRANSACTION INTERFACE
        //////////
        public boolean prepare(int xid)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        public boolean lm_unlockAll(int xid) throws RemoteException;

        /**
         * Commit transaction.
         *
         * @param xid id of transaction to be committed.
         * @return true on success, false on failure.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         */
        public boolean commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /**
         * Abort transaction.
         *
         * @param xid id of transaction to be aborted.
         *
         * @throws RemoteException             on communications failure.
         * @throws InvalidTransactionException if transaction id is invalid.
         */
        public void abort(int xid) throws RemoteException, InvalidTransactionException;

        //////////
        // ADMINISTRATIVE INTERFACE
        //////////
        /**
         * Add seats to a flight. In general this will be used to create a new flight,
         * but it should be possible to add seats to an existing flight. Adding to an
         * existing flight should overwrite the current price of the available seats.
         *
         * @param xid       id of transaction.
         * @param flightNum flight number, cannot be null.
         * @param numSeats  number of seats to be added to the flight.
         * @param price     price of each seat. If price < 0, don't overwrite the
         *                  current price.
         * @return true on success, false on failure.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         */
        public boolean addFlight(int xid, String flightNum, int numSeats, int price)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /**
         * Delete an entire flight. Implies whole deletion of the flight: all seats, all
         * reservations. Should fail if a customer has a reservation on this flight.
         *
         * @param xid       id of transaction.
         * @param flightNum flight number, cannot be null.
         * @return true on success, false on failure.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         */
        public boolean deleteFlight(int xid, String flightNum)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /**
         * Add rooms to a location. This should look a lot like addFlight, only keyed on
         * a location instead of a flight number.
         *
         * @return true on success, false on failure.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         *
         * @see #addFlight
         */
        public boolean addRooms(int xid, String location, int numRooms, int price)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /**
         * Delete rooms from a location. This subtracts from the available room count
         * (rooms not allocated to a customer). It should fail if it would make the
         * count of available rooms negative.
         *
         * @return true on success, false on failure.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         *
         * @see #deleteFlight
         */
        public boolean deleteRooms(int xid, String location, int numRooms)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /**
         * Add cars to a location. Cars have the same semantics as hotels (see
         * addRooms).
         * 
         * @return true on success, false on failure.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         *
         * @see #addRooms
         * @see #addFlight
         */
        public boolean addCars(int xid, String location, int numCars, int price)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /**
         * Delete cars from a location. Cars have the same semantics as hotels.
         *
         * @return true on success, false on failure.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         *
         * @see #deleteRooms
         * @see #deleteFlight
         */
        public boolean deleteCars(int xid, String location, int numCars)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /**
         * Add a new customer to database. Should return success if customer already
         * exists.
         * 
         * @param xid      id of transaction.
         * @param custName name of customer.
         * @return true on success, false on failure.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         */
        public boolean newCustomer(int xid, String custName)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /**
         * Delete this customer and associated reservations.
         *
         * @param xid      id of transaction.
         * @param custName name of customer.
         * @return true on success, false on failure.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         */
        public boolean deleteCustomer(int xid, String custName)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        //////////
        // QUERY INTERFACE
        //////////
        /**
         * Return the number of empty seats on a flight.
         *
         * @param xid       id of transaction.
         * @param flightNum flight number.
         * @return # empty seats on the flight.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         */
        public int queryFlight(int xid, String flightNum)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /** Return the price of a seat on this flight. */
        public int queryFlightPrice(int xid, String flightNum)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /** Return the number of rooms available at a location. */
        public int queryRooms(int xid, String location)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /** Return the price of rooms at this location. */
        public int queryRoomsPrice(int xid, String location)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /** Return the number of cars available at a location. */
        public int queryCars(int xid, String location)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /** Return the price of rental cars at this location. */
        public int queryCarsPrice(int xid, String location)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        public ArrayList<ResvPair> queryCustomerResv(int xid, String custName)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        //////////
        // RESERVATION INTERFACE
        //////////
        /**
         * Reserve a flight on behalf of this customer.
         *
         * @param xid       id of transaction.
         * @param custName  name of customer.
         * @param flightNum flight number.
         * @return true on success, false on failure.
         *
         * @throws RemoteException             on communications failure.
         * @throws TransactionAbortedException if transaction was aborted.
         * @throws InvalidTransactionException if transaction id is invalid.
         */
        public boolean reserveFlight(int xid, String custName, String flightNum)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /** Reserve a car for this customer at the specified location. */
        public boolean reserveCar(int xid, String custName, String location)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        /** Reserve a room for this customer at the specified location. */
        public boolean reserveRoom(int xid, String custName, String location)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        public boolean reserveCustomer(int xid, String custName, int resvType, String resvKey)
                        throws RemoteException, TransactionAbortedException, InvalidTransactionException;

        //////////
        // TECHNICAL/TESTING INTERFACE
        //////////
        /**
         * Shutdown gracefully. Stop accepting new transactions, wait for running
         * transactions to terminate, and clean up disk state. When this RM restarts, it
         * should not attempt to recover its state if the client called shutdown to
         * terminate it.
         *
         * @return true on success, false on failure.
         * @throws RemoteException on communications failure.
         */
        public boolean shutdown() throws RemoteException;

        /**
         * Call exit immediately. Used to simulate a system failure such as a power
         * outage.
         * <p>
         * This method is used for testing and is not part of a transaction.
         *
         * @return true on success, false on failure.
         */
        public boolean dieNow() throws RemoteException;

        /**
         * Sets a flag so that the RM fails in the middle of the next commit operation.
         * Specifically, using shadow paging, you should call System.exit() after the
         * updated tables have been written to disk, but just BEFORE you switch the
         * on-disk pointer to point to this new copy. (Convince yourself that the
         * transaction is effectively aborted.)
         * <p>
         * This method is used for testing and is not part of a transaction.
         *
         * @return true on success, false on failure.
         */
        public boolean dieBeforePointerSwitch() throws RemoteException;

        /**
         * Sets a flag so that the RM fails in the middle of the next commit operation.
         * Unlike the above, however, the RM exits AFTER the on-disk pointer is updated,
         * but before the commit call could return to the caller. Make sure that, when
         * the RM recovers, the transaction is seen as committed.
         * <p>
         * This method is used for testing and is not part of a transaction.
         *
         * @return true on success, false on failure.
         */
        public boolean dieAfterPointerSwitch() throws RemoteException;

        /**
         * Sets a flag so that the RM fails after the next enlist() operation. That is,
         * the RM immediately dies on return of the enlist() call it made to the TM,
         * before it could fulfil the client's query/reservation request.
         * <p>
         * This method is used for testing and is not part of a transaction.
         *
         * @return true on success, false on failure.
         */
        public boolean dieRMAfterEnlist() throws RemoteException;

        /**
         * Sets a flag so that the RM fails when it next tries to prepare, but before it
         * gets a chance to save the update list to disk.
         * <p>
         * This method is used for testing and is not part of a transaction.
         *
         * @return true on success, false on failure.
         */
        public boolean dieRMBeforePrepare() throws RemoteException;

        /**
         * Sets a flag so that the RM fails when it next tries to prepare: after it has
         * entered the prepared state, but just before it could reply "prepared" to the
         * TM.
         * <p>
         * This method is used for testing and is not part of a transaction.
         *
         * @return true on success, false on failure.
         */
        public boolean dieRMAfterPrepare() throws RemoteException;

        /**
         * Sets a flag so that the RM fails when it is told by the TM to commit, by
         * before it could actually change the database content (i.e., die at beginning
         * of the commit() function called by TM).
         * <p>
         * This method is used for testing and is not part of a transaction.
         *
         * @return true on success, false on failure.
         */
        public boolean dieRMBeforeCommit() throws RemoteException;

        /**
         * Sets a flag so that the RM fails when it is told by the TM to abort, by
         * before it could actually do anything. (i.e., die at beginning of the abort()
         * function called by TM).
         * <p>
         * This method is used for testing and is not part of a transaction.
         *
         * @return true on success, false on failure.
         */
        public boolean dieRMBeforeAbort() throws RemoteException;

}
