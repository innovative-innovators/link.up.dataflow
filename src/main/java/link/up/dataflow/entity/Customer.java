package link.up.dataflow.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Vincent on 2018/2/11.
 */

@Data
public class Customer implements Serializable {

    private String id;
    private String name;
    private List<CreditCard> creditCards;
}

