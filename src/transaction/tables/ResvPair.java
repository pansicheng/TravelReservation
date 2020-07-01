package transaction.tables;

import java.io.Serializable;

public class ResvPair implements Serializable {

	private static final long serialVersionUID = 1L;
    private int resvType;
    private String resvKey;

    public ResvPair(int resvType, String resvKey) {
        this.resvType = resvType;
        this.resvKey = resvKey;
    }

    public int getResvType() {
        return resvType;
    }

    public String getResvKey() {
        return resvKey;
    }

}
