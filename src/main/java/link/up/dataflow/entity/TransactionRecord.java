package link.up.dataflow.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by Vincent on 2018/2/12.
 */
@Data
public class TransactionRecord implements Serializable {

    private int step;
    private String type;
    private double amount;
    private String nameOrig;
    private double oldbalanceOrg;
    private double newbalanceOrig;
    private String nameDest;
    private double oldbalanceDest;
    private double newbalanceDest;
    private int isFraud;
    private int isFlaggedFraud;


    @Override
    public String toString() {
        return "Transaction happened from " + nameOrig + " to " + nameDest + ", with amount :" + amount;
    }

}
