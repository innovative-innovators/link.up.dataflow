package link.up.dataflow.entity;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by Vincent on 2018/2/12.
 */


@Data
public class CreditCard implements Serializable {

    private String accountNumber;
    private String cardNumber;
    private String expirationDate;
    private String customerId;

}
