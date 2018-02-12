package link.up.dataflow.utils;

import org.neo4j.driver.v1.*;
import org.slf4j.LoggerFactory;

/**
 * Created by Vincent on 2018/2/12.
 */
public class Neo4JOpThread implements Runnable {

    private Driver driver;
    private org.slf4j.Logger logger = LoggerFactory.getLogger(Neo4JOpThread.class);
    private String statement;
    private String logMsg;
    private Session session;

    public Neo4JOpThread(String url, String userName, String password) {
        this.driver = GraphDatabase.driver(url, AuthTokens.basic(userName, password));
        session = driver.session();
    }

    public Neo4JOpThread setStatement(String statement) {
        this.statement = statement;
        return this;
    }

    public Neo4JOpThread setLogMsg(String logMsg) {
        this.logMsg = logMsg;
        return this;
    }

    @Override
    public void run() {

        try (Transaction txn = session.beginTransaction()) {

            txn.run(statement);

            txn.success(); // Mark this write as successful.

            logger.info(logMsg);

        }

    }
}
