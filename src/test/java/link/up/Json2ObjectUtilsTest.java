package link.up;

import link.up.dataflow.entity.TransactionRecord;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by Vincent on 2018/2/12.
 */
public class Json2ObjectUtilsTest {

    @Test
    public void test() throws IOException {

        String jsonStr = "{\"oldbalanceOrg\": \"53973.0\", \"newbalanceDest\": \"1441843.94\", \"oldbalanceDest\": \"1972858.8\", \"nameOrig\": \"C2011483062\", \"amount\": \"343626.18\", \"nameDest\": \"C273192837\", \"isFraud\": \"0\", \"newbalanceOrig\": \"397599.18\", \"type\": \"CASH_IN\"}";

        ObjectMapper objectMapper = new ObjectMapper();

        TransactionRecord t = objectMapper.readValue(jsonStr, TransactionRecord.class);

        System.out.println(t.toString());
    }

}
