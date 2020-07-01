package transaction.tables;

import java.io.Serializable;
import java.util.HashMap;

/**
 * All hotel rooms in a given location cost the same; location is a primary key
 * for HOTELS.
 */
public class HotelsTable implements Serializable {

    private static final long serialVersionUID = 1L;
    private HashMap<String, HotelsRow> hotelsTable;

    public HotelsTable() {
        hotelsTable = new HashMap<String, HotelsRow>();
    }

    /**
     * Add rooms to a location.
     */
    public boolean addRooms(String location, int numRooms, int price) {
        if (hotelsTable.containsKey(location)) {
            HotelsRow hotelsRow = hotelsTable.get(location);
            if (price >= 0)
                hotelsRow.setPrice(price);
            hotelsRow.setNumRooms(numRooms + hotelsRow.getNumRooms());
            hotelsRow.setNumAvail(numRooms + hotelsRow.getNumAvail());
            hotelsTable.put(location, hotelsRow);
        } else
            hotelsTable.put(location, new HotelsRow(location, price, numRooms, numRooms));
        return true;
    }

    /**
     * Delete rooms from a location.
     */
    public boolean deleteRooms(String location, int numRooms) {
        if (hotelsTable.containsKey(location)) {
            HotelsRow hotelsRow = hotelsTable.get(location);
            if (hotelsRow.getNumAvail() < numRooms)
                return false;
            hotelsRow.setNumRooms(hotelsRow.getNumRooms() - numRooms);
            hotelsRow.setNumAvail(hotelsRow.getNumAvail() - numRooms);
            hotelsTable.put(location, hotelsRow);
            return true;
        }
        return false;
    }

    /**
     * Return the number of rooms available at a location.
     */
    public int queryRooms(String location) {
        if (hotelsTable.containsKey(location))
            return hotelsTable.get(location).getNumAvail();
        return -1;
    }

    /**
     * Return the price of rooms at this location.
     */
    public int queryRoomsPrice(String location) {
        if (hotelsTable.containsKey(location))
            return hotelsTable.get(location).getPrice();
        return -1;
    }

    public boolean reserveRoom(String location) {
        if (hotelsTable.containsKey(location)) {
            HotelsRow hotelsRow = hotelsTable.get(location);
            if (hotelsRow.getNumAvail() > 0) {
                hotelsRow.setNumAvail(hotelsRow.getNumAvail() - 1);
                hotelsTable.put(location, hotelsRow);
                return true;
            }
        }
        return false;
    }

    public boolean cancelRoom(String location) {
        if (hotelsTable.containsKey(location)) {
            HotelsRow hotelsRow = hotelsTable.get(location);
            hotelsRow.setNumAvail(hotelsRow.getNumAvail() + 1);
            hotelsTable.put(location, hotelsRow);
            return true;
        }
        return false;
    }

    public boolean containsKey(String location) {
        return hotelsTable.containsKey(location);
    }

    public HotelsRow get(String location) {
        return hotelsTable.get(location);
    }

    public HotelsRow put(String location, HotelsRow hotelsRow) {
        return hotelsTable.put(location, hotelsRow);
    }

    public void remove(String location) {
        hotelsTable.remove(location);
    }
}
