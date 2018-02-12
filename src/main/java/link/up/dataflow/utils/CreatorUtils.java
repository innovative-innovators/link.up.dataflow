package link.up.dataflow.utils;

import link.up.dataflow.entity.CreditCard;
import link.up.dataflow.entity.Customer;

/**
 * Created by Vincent on 2018/2/12.
 */
public class CreatorUtils {

    private CreatorUtils() {
    }


    /**
     * According to record line to create Customer
     * <p>
     * Field sequence
     * <p>
     * -customer_id
     * -customer_name
     * -customer_address
     * -birthday
     * -average_yearly_income
     * -country
     * </p>
     *
     * @param line
     * @return
     */
    public static Customer createCustomer(String line) {

        String[] cells = line.split(",");

        Customer c = new Customer();
        c.setId(cells[0]);
        c.setName(cells[1]);

        return c;

    }


    /**
     * According to Account-assoc to create CreditCard
     * <p>
     * -account_number
     * -customer_id
     * -credit_card_number
     * -credit_card_expire_date
     *
     * @param line
     * @return
     */
    public static CreditCard createCreditCard(String line) {

        String[] cells = line.split(",");

        CreditCard c = new CreditCard();
        c.setAccountNumber(cells[0]);
        c.setCustomerId(cells[1]);
        c.setCardNumber(cells[2]);
        c.setExpirationDate(cells[3]);

        return c;
    }

}
