package link.up.dataflow.utils;

import link.up.dataflow.entity.TransactionRecord;
import org.apache.avro.data.Json;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by Vincent on 2018/2/12.
 */
public class Json2ObjectUtils {

    private static ObjectMapper objectMapper = new ObjectMapper();

    private Json2ObjectUtils() {
    }

    public static TransactionRecord convertTo(String jsonStr) throws IOException {

        return objectMapper.readValue(jsonStr, TransactionRecord.class);
    }

}
