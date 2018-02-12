package link.up.dataflow.utils;

import link.up.dataflow.entity.CreditCard;
import link.up.dataflow.entity.Customer;
import link.up.dataflow.entity.TransactionRecord;
import org.neo4j.driver.v1.*;
import org.slf4j.*;

import static org.neo4j.driver.v1.Values.parameters;

/**
 * Created by Vincent on 2018/2/12.
 */
public class Neo4JUtils {

    private static Driver driver;
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(Neo4JUtils.class);

    public final static String RELATIONSHIP_NAME = "TRANSFER_TO";

    //TODO
    /**
     * Using "parameters()"
     */
    // NEO4J format
    // public final static String MERGE_STATEMENT = "MERGE (accountFrom:Account {accountNum:{accountNum}})-[r:%s {amount:{amount}}]->(accountTo:Account {accountNum:{accountNum2}})";

    // Java format
    public final static String MERGE_STATEMENT_1 = "MERGE (accountFrom:Account {accountNum:%s})-[r:%s {amount:%f}]->(accountTo:Account {accountNum:%s})";
    public final static String MERGE_STATEMENT_2 = "MERGE (custA:Customer {custId:%s,name:%s})-[:OWNS]->(acctA:Account {accountNumber:%s, cardNumber:%s , expiration:%s})-[r:%s {amount:%f}]->(acctB:Account {accountNumber:%s, cardNumber:%s , expiration:%s})<-[:OWNS]-(custB:Customer {custId:%s,name:%s})";

    private Neo4JUtils() {

    }


    /**
     * (A)-[:RELATIONSHIP]->(B)
     *
     * @param record
     * @return
     */
    public static String manipulateRelationshipFromTransactionRecord(TransactionRecord record) {
        return String.format(MERGE_STATEMENT_1,
                "'" + record.getNameOrig() + "'",
                RELATIONSHIP_NAME,
                record.getAmount(),
                "'" + record.getNameDest() + "'"
        );
    }


    /**
     * Manipulate Relationship between CUSTOMER A -[:OWNS]- ACCOUNT A--> [:TRANSFER_TO] --> ACCOUNT B -[:OWNS]- CUSTOMER B
     * <p>
     * *    MERGE (custA:Customer {custId:'A',name:'A'})
     * *          -[:OWNS]->
     * *        (acctA:Account {accountNumber:"aa"})
     * *          -[:TRANSFER_TO {amount:xx}]->
     * *        (acctB:Account {accountNumber:"bb"})
     * *          <-[:OWNS]-
     * *        (custB:Customer {custId:'B',name:'B'})
     *
     * @param record
     * @return
     */
    public static String manipulateRelationshipFromTransactionRecord(TransactionRecord record,
                                                                     Customer customerA,
                                                                     CreditCard creditCardA,
                                                                     Customer customerB,
                                                                     CreditCard creditCardB) {

        return String.format(MERGE_STATEMENT_2,
                "'" + customerA.getId() + "'",
                "'" + customerA.getName() + "'",
                "'" + creditCardA.getAccountNumber() + "'",
                "'" + creditCardA.getCardNumber() + "'",
                "'" + creditCardA.getExpirationDate() + "'",
                RELATIONSHIP_NAME,
                record.getAmount(),
                "'" + creditCardB.getAccountNumber() + "'",
                "'" + creditCardB.getCardNumber() + "'",
                "'" + creditCardB.getExpirationDate() + "'",
                "'" + customerB.getId() + "'",
                "'" + customerB.getName() + "'"
        );
    }

}
