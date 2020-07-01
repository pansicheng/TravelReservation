package transaction.tables;

import java.io.Serializable;
import java.util.HashMap;

/**
 * There is only one airline, and all seats on a given flight have the same
 * price; flightNum is a primary key for FLIGHTS.
 * 
 * In the FLIGHTS table, numAvail is the number of seats available to be booked
 * on a given flight. For a given flightNum, one of the database consistency
 * conditions is that the total number of reservations for a flight (in the
 * RESERVATIONS table) plus the number of available seats must add up to the
 * total number of seats in the flight. Similar conditions hold for the CARS and
 * HOTELS tables.
 */
public class FlightsTable implements Serializable {

    private static final long serialVersionUID = 1L;
    private HashMap<String, FlightsRow> flightsTable;

    public FlightsTable() {
        flightsTable = new HashMap<String, FlightsRow>();
    }

    /**
     * Add seats to a flight.
     */
    public boolean addFlight(String flightNum, int numSeats, int price) {
        if (flightsTable.containsKey(flightNum)) {
            FlightsRow flightsRow = flightsTable.get(flightNum);
            if (price >= 0)
                flightsRow.setPrice(price);
            flightsRow.setNumSeats(numSeats + flightsRow.getNumSeats());
            flightsRow.setNumAvail(numSeats + flightsRow.getNumAvail());
            flightsTable.put(flightNum, flightsRow);
        } else
            flightsTable.put(flightNum, new FlightsRow(flightNum, price, numSeats, numSeats));
        return true;
    }

    /**
     * Delete an entire flight.
     */
    public boolean deleteFlight(String flightNum) {
        if (flightsTable.containsKey(flightNum)) {
            flightsTable.remove(flightNum);
            return true;
        }
        return false;
    }

    /**
     * Return the number of empty seats on a flight.
     */
    public int queryFlight(String flightNum) {
        if (flightsTable.containsKey(flightNum))
            return flightsTable.get(flightNum).getNumAvail();
        return -1;
    }

    /**
     * Return the price of a seat on this flight.
     */
    public int queryFlightPrice(String flightNum) {
        if (flightsTable.containsKey(flightNum))
            return flightsTable.get(flightNum).getPrice();
        return -1;
    }

    public boolean reserveFlight(String flightNum) {
        if (flightsTable.containsKey(flightNum)) {
            FlightsRow flightsRow = flightsTable.get(flightNum);
            if (flightsRow.getNumAvail() > 0) {
                flightsRow.setNumAvail(flightsRow.getNumAvail() - 1);
                flightsTable.put(flightNum, flightsRow);
                return true;
            }
        }
        return false;
    }

    public boolean cancelFlight(String flightNum) {
        if (flightsTable.containsKey(flightNum)) {
            FlightsRow flightsRow = flightsTable.get(flightNum);
            flightsRow.setNumAvail(flightsRow.getNumAvail() + 1);
            flightsTable.put(flightNum, flightsRow);
            return true;
        }
        return false;
    }

    public boolean containsKey(String flightNum) {
        return flightsTable.containsKey(flightNum);
    }

    public FlightsRow get(String flightNum) {
        return flightsTable.get(flightNum);
    }

    public void put(String flightNum, FlightsRow flightsRow) {
        flightsTable.put(flightNum, flightsRow);
    }

}
