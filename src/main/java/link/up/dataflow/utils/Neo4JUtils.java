package link.up.dataflow.utils;

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
    public final static String MERGE_STATEMENT = "MERGE (accountFrom:Account {accountNum:%s})-[r:%s {amount:%f}]->(accountTo:Account {accountNum:%s})";

    private Neo4JUtils() {

    }


    /**
     * (A)-[:RELATIONSHIP]->(B)
     *
     * @param record
     * @return
     */
    public static String manipulateRelationshipFromTransactionRecord(TransactionRecord record) {
        return String.format(MERGE_STATEMENT,
                "'" + record.getNameOrig() + "'",
                RELATIONSHIP_NAME,
                record.getAmount(),
                "'" + record.getNameDest() + "'"
        );
    }

}
