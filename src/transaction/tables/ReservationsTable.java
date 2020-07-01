package transaction.tables;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * custName is a primary key for CUSTOMERS.
 * 
 * The RESERVATIONS table contains an entry corresponding to each reservation
 * made by a customer for a flight, car, or hotel, as follows: resvType
 * indicates the type of reservation (1 for a flight, 2 for a hotel room, and 3
 * for a car), and resvKey is the primary key of the corresponding reserved
 * item.
 * 
 * The RESERVATIONS table doesn't have a primary key, but it's mostly looked up
 * by the custName; so one possible implementation is to combine it with the
 * CUSTOMERS table. That is, have a hashtable indexed by custName, with each
 * hashtable entry containing a list of (resvType, resvKey) pairs.
 */
public class ReservationsTable implements Serializable {

    private static final long serialVersionUID = 1L;
    private HashMap<String, ArrayList<ResvPair>> reservationsTable;
    public final static int resvTypeFlight = 1;
    public final static int resvTypeHotelRoom = 2;
    public final static int resvTypeCar = 3;

    public ReservationsTable() {
        reservationsTable = new HashMap<String, ArrayList<ResvPair>>();
    }

    /**
     * Delete this customer and associated reservations.
     */
    public boolean deleteCustomer(String custName) {
        if (reservationsTable.containsKey(custName)) {
            reservationsTable.remove(custName);
            return true;
        }
        return false;
    }

    /**
     * Add a new customer to database.
     */
    public boolean newCustomer(String custName) {
        if (reservationsTable.containsKey(custName))
            return false;
        reservationsTable.put(custName, new ArrayList<ResvPair>());
        return true;
    }

    public boolean addResvPair(String custName, int resvType, String resvKey) {
        if (reservationsTable.containsKey(custName)) {
            ArrayList<ResvPair> resvPairs = reservationsTable.get(custName);
            resvPairs.add(new ResvPair(resvType, resvKey));
            reservationsTable.put(custName, resvPairs);
        } else {
            ArrayList<ResvPair> resvPairs = new ArrayList<ResvPair>();
            resvPairs.add(new ResvPair(resvType, resvKey));
            reservationsTable.put(custName, resvPairs);
        }
        return true;
    }

    public boolean containsKey(String custName) {
        return reservationsTable.containsKey(custName);
    }

    public ArrayList<ResvPair> get(String custName) {
        return reservationsTable.get(custName);
    }

    public ArrayList<ResvPair> getClone(String custName) {
        ArrayList<ResvPair> clone = new ArrayList<ResvPair>();
        for (ResvPair resvPair : reservationsTable.get(custName)) {
            clone.add(new ResvPair(resvPair.getResvType(), resvPair.getResvKey()));
        }
        return clone;
    }

    public void put(String custName, ArrayList<ResvPair> resvPairs) {
        reservationsTable.put(custName, resvPairs);
    }
}
