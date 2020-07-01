package transaction.tables;

import java.io.Serializable;
import java.util.HashMap;

/**
 * All rental cars in a given location cost the same; location is a primary key
 * for CARS.
 */
public class CarsTable implements Serializable {

    private static final long serialVersionUID = 1L;
    private HashMap<String, CarsRow> carsTable;

    public CarsTable() {
        carsTable = new HashMap<String, CarsRow>();
    }

    /**
     * Add cars to a location.
     */
    public boolean addCars(String location, int numCars, int price) {
        if (carsTable.containsKey(location)) {
            CarsRow carsRow = carsTable.get(location);
            if (price >= 0)
                carsRow.setPrice(price);
            carsRow.setNumCars(numCars + carsRow.getNumCars());
            carsRow.setNumAvail(numCars + carsRow.getNumAvail());
            carsTable.put(location, carsRow);
        } else
            carsTable.put(location, new CarsRow(location, price, numCars, numCars));
        return true;
    }

    /**
     * Delete cars from a location.
     */
    public boolean deleteCars(String location, int numCars) {
        if (carsTable.containsKey(location)) {
            CarsRow carsRow = carsTable.get(location);
            // Delete from available.
            if (carsRow.getNumAvail() < numCars)
                return false;
            carsRow.setNumCars(carsRow.getNumCars() - numCars);
            carsRow.setNumAvail(carsRow.getNumAvail() - numCars);
            carsTable.put(location, carsRow);
            return true;
        }
        return false;
    }

    /**
     * Return the number of cars available at a location.
     */
    public int queryCars(String location) {
        if (carsTable.containsKey(location))
            return carsTable.get(location).getNumAvail();
        return -1;
    }

    /**
     * Return the price of rental cars at this location.
     */
    public int queryCarsPrice(String location) {
        if (carsTable.containsKey(location))
            return carsTable.get(location).getPrice();
        return -1;
    }

    public boolean reserveCar(String location) {
        if (carsTable.containsKey(location)) {
            CarsRow carsRow = carsTable.get(location);
            if (carsRow.getNumAvail() > 0) {
                carsRow.setNumAvail(carsRow.getNumAvail() - 1);
                carsTable.put(location, carsRow);
                return true;
            }
        }
        return false;
    }

    public boolean cancelCar(String location) {
        if (carsTable.containsKey(location)) {
            CarsRow carsRow = carsTable.get(location);
            carsRow.setNumAvail(carsRow.getNumAvail() + 1);
            carsTable.put(location, carsRow);
            return true;
        }
        return false;
    }

    public boolean containsKey(String location) {
        return carsTable.containsKey(location);
    }

    public CarsRow get(String location) {
        return carsTable.get(location);
    }

    public void put(String location, CarsRow carsRow) {
        carsTable.put(location, carsRow);
    }

    public void remove(String location) {
        carsTable.remove(location);
    }

}
