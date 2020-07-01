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
import java.security.Key;
import java.util.HashMap;
import java.util.concurrent.locks.Lock;
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
 * Description: toy implementation of the RM, for initial testing
 */

public class ResourceManagerImpl extends java.rmi.server.UnicastRemoteObject implements ResourceManager {

	private static final long serialVersionUID = 1L;

	protected int xidCounter;

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

	private static LockManager lm;
	private HashMap<Integer, ArrayList<TableModified>> activeTransactions;

	private boolean dieBefore = false;
	private boolean dieAfter = false;

	public static void main(String args[]) {
		System.setSecurityManager(new RMISecurityManager());

		String rmiName = System.getProperty("rmiName");
		if (rmiName == null || rmiName.equals("")) {
			rmiName = ResourceManager.DefaultRMIName;
		}

		String rmiRegPort = System.getProperty("rmiRegPort");
		if (rmiRegPort != null && !rmiRegPort.equals("")) {
			rmiName = "//:" + rmiRegPort + "/" + rmiName;
		}

		try {
			ResourceManagerImpl obj = new ResourceManagerImpl();
			Naming.rebind(rmiName, obj);
			System.out.println("RM bound");
		} catch (Exception e) {
			System.err.println("RM not bound:" + e);
			System.exit(1);
		}
	}

	public ResourceManagerImpl() throws RemoteException {
		checkDataDir();
		recover();
		activeTransactions = new HashMap<Integer, ArrayList<TableModified>>();
		xidCounter = 0;
		lm = new LockManager();
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
		if (!activeTransactions.containsKey(xid) && activeTransactions.size() == 0)
			throw new TransactionAbortedException(xid, "TransactionAbortedException");
		else if (!activeTransactions.containsKey(xid))
			throw new InvalidTransactionException(xid, "Bogus xid.");
	}

	private void recover() {
		File file;

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
		}

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
		ArrayList<TableModified> tableModifieds = activeTransactions.get(xid);
		tableModifieds.add(new TableModified(Table, TableKey));
		activeTransactions.put(xid, tableModifieds);
	}

	// TRANSACTION INTERFACE
	public int start() throws RemoteException {
		xidCounter++;
		activeTransactions.put(xidCounter, new ArrayList<TableModified>());
		return xidCounter;
	}

	public boolean commit(int xid) throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		System.out.println("Committing");

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

		if (dieBefore)
			System.exit(0);

		if (MF) {
			store(FLIGHTS_DB, flightsTable);
			store(A_FLIGHTS_DB, aFlightsTable);
		}
		if (MH) {
			store(HOTELS_DB, hotelsTable);
			store(A_HOTELS_DB, aHotelsTable);
		}
		if (MC) {
			store(CARS_DB, carsTable);
			store(A_CARS_DB, aCarsTable);
		}
		if (MR) {
			store(RESERVATIONS_DB, reservationsTable);
			store(A_RESERVATIONS_DB, aReservationsTable);
		}

		if (dieAfter)
			System.exit(0);

		activeTransactions.remove(xid);
		lm.unlockAll(xid);

		return true;
	}

	public void abort(int xid) throws RemoteException, InvalidTransactionException {

		undo(xid);

		activeTransactions.remove(xid);
		lm.unlockAll(xid);

	}

	// ADMINISTRATIVE INTERFACE
	public boolean addFlight(int xid, String flightNum, int numSeats, int price)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyFlight + flightNum, LockManager.WRITE)) {
				updateActiveTransactions(xid, KeyFlight, flightNum);
				return aFlightsTable.addFlight(flightNum, numSeats, price);
			} else
				return false;
		} catch (DeadlockException e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public boolean deleteFlight(int xid, String flightNum)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyFlight + flightNum, LockManager.WRITE))
				if (aFlightsTable.containsKey(flightNum))
					if (aFlightsTable.get(flightNum).getNumSeats() == aFlightsTable.get(flightNum).getNumAvail()) {
						updateActiveTransactions(xid, KeyFlight, flightNum);
						return aFlightsTable.deleteFlight(flightNum);
					}
			return false;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public boolean addRooms(int xid, String location, int numRooms, int price)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyHotel + location, LockManager.WRITE)) {
				updateActiveTransactions(xid, KeyHotel, location);
				return aHotelsTable.addRooms(location, numRooms, price);
			} else
				return false;
		} catch (DeadlockException e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public boolean deleteRooms(int xid, String location, int numRooms)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyHotel + location, LockManager.WRITE))
				if (aHotelsTable.containsKey(location))
					if (aHotelsTable.get(location).getNumRooms() == aHotelsTable.get(location).getNumAvail()) {
						updateActiveTransactions(xid, KeyHotel, location);
						return aHotelsTable.deleteRooms(location, numRooms);
					}
			return false;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public boolean addCars(int xid, String location, int numCars, int price)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyCar + location, LockManager.WRITE)) {
				updateActiveTransactions(xid, KeyCar, location);
				return aCarsTable.addCars(location, numCars, price);
			} else
				return false;
		} catch (DeadlockException e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public boolean deleteCars(int xid, String location, int numCars)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyCar + location, LockManager.WRITE))
				if (aCarsTable.containsKey(location))
					if (aCarsTable.get(location).getNumCars() == aCarsTable.get(location).getNumAvail()) {
						updateActiveTransactions(xid, KeyCar, location);
						return aCarsTable.deleteCars(location, numCars);
					}
			return false;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public boolean newCustomer(int xid, String custName)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyReservation + custName, LockManager.WRITE)) {
				updateActiveTransactions(xid, KeyReservation, custName);
				return aReservationsTable.newCustomer(custName);
			} else
				return false;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public boolean deleteCustomer(int xid, String custName)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
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
							abort(xid);
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
							abort(xid);
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
							abort(xid);
							throw new TransactionAbortedException(xid, "Transaction aborted by deadlock issue");
						}
					} else
						return false;
				}
				updateActiveTransactions(xid, KeyReservation, custName);
				return aReservationsTable.deleteCustomer(custName);
			} else
				return false;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	// QUERY INTERFACE
	public int queryFlight(int xid, String flightNum)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyFlight + flightNum, LockManager.READ))
				return aFlightsTable.queryFlight(flightNum);
			else
				return -1;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public int queryFlightPrice(int xid, String flightNum)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyFlight + flightNum, LockManager.READ))
				return aFlightsTable.queryFlightPrice(flightNum);
			else
				return -1;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public int queryRooms(int xid, String location)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyHotel + location, LockManager.READ))
				return aHotelsTable.queryRooms(location);
			else
				return -1;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public int queryRoomsPrice(int xid, String location)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyHotel + location, LockManager.READ))
				return aHotelsTable.queryRoomsPrice(location);
			else
				return -1;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public int queryCars(int xid, String location)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyCar + location, LockManager.READ))
				return aCarsTable.queryCars(location);
			else
				return -1;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public int queryCarsPrice(int xid, String location)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyCar + location, LockManager.READ))
				return aCarsTable.queryCarsPrice(location);
			else
				return -1;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public int queryCustomerBill(int xid, String custName)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyReservation + custName, LockManager.WRITE))
				if (aReservationsTable.containsKey(custName)) {
					int bill = 0;
					for (ResvPair resvPair : aReservationsTable.get(custName))
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
					return bill;
				}
			return -1;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	// RESERVATION INTERFACE
	public boolean reserveFlight(int xid, String custName, String flightNum)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyFlight + flightNum, LockManager.WRITE)
					&& lm.lock(xid, KeyReservation + custName, LockManager.WRITE)) {
				updateActiveTransactions(xid, KeyFlight, flightNum);
				updateActiveTransactions(xid, KeyReservation, custName);
				return aFlightsTable.reserveFlight(flightNum)
						&& aReservationsTable.addResvPair(custName, ReservationsTable.resvTypeFlight, flightNum);
			}
			return false;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public boolean reserveCar(int xid, String custName, String location)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyCar + location, LockManager.WRITE)
					&& lm.lock(xid, KeyReservation + custName, LockManager.WRITE)) {
				updateActiveTransactions(xid, KeyCar, location);
				updateActiveTransactions(xid, KeyReservation, custName);
				return aCarsTable.reserveCar(location)
						&& aReservationsTable.addResvPair(custName, ReservationsTable.resvTypeCar, location);
			}
			return false;
		} catch (Exception e) {
			abort(xid);
			throw new TransactionAbortedException(xid, "DeadlockException");
		}
	}

	public boolean reserveRoom(int xid, String custName, String location)
			throws RemoteException, TransactionAbortedException, InvalidTransactionException {
		checkXid(xid);
		try {
			if (lm.lock(xid, KeyHotel + location, LockManager.WRITE)
					&& lm.lock(xid, KeyReservation + custName, LockManager.WRITE)) {
				updateActiveTransactions(xid, KeyHotel, location);
				updateActiveTransactions(xid, KeyReservation, custName);
				return aHotelsTable.reserveRoom(location)
						&& aReservationsTable.addResvPair(custName, ReservationsTable.resvTypeHotelRoom, location);
			}
			return false;
		} catch (Exception e) {
			abort(xid);
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

	public boolean dieBeforePointerSwitch() throws RemoteException {
		dieBefore = true;
		return true;
	}

	public boolean dieAfterPointerSwitch() throws RemoteException {
		dieAfter = true;
		return true;
	}

}
