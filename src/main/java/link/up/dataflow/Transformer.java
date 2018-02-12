package link.up.dataflow;

import link.up.dataflow.entity.TransactionRecord;
import link.up.dataflow.utils.Json2ObjectUtils;
import link.up.dataflow.utils.Neo4JOpThread;
import link.up.dataflow.utils.Neo4JUtils;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Vincent on 2018/2/11.
 */
public class Transformer {

    public static String TOPIC_NAME_TEMPLATE = "projects/PROJECT_NAME/topics/TOPIC_NAME";
    private static Logger logger = LoggerFactory.getLogger(Transformer.class);

    public interface MyOptions extends DataflowPipelineOptions {

        String getTopic();

        void setTopic(String topic);

        String getNeo4jUrl();

        void setNeo4jUrl(String neo4jUrl);

        String getUserName();

        void setUserName(String userName);

        String getPassword();

        void setPassword(String password);

    }

    public static void main(String args[]) {

        String projectId = args[0].split("=")[1];

        String topicName = TOPIC_NAME_TEMPLATE.replace("PROJECT_NAME", projectId)
                .replace("TOPIC_NAME", args[1].split("=")[1]);

        MyOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        myOptions.setStreaming(true);
        myOptions.setJobName("Pubsub-Linkup-demo");

        String neo4jurl = myOptions.getNeo4jUrl();
        String neo4jUserName = myOptions.getUserName();
        String neo4jPassword = myOptions.getPassword();


        /**
         *  Create PIPELINE
         */
        Pipeline pipeline = Pipeline.create(myOptions);


        /**
         *  Read from PubSub
         */

        PCollection<TransactionRecord> txnInput = pipeline
                .apply("ReceiveTransaction", PubsubIO.readStrings().fromTopic(topicName))
                .apply("TimeWindow",
                        Window.<String>into(FixedWindows.of(Duration.millis(500)))
                                .triggering(
                                        AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.millis(500))
                                ).withAllowedLateness(Duration.millis(100))
                                .accumulatingFiredPanes()
                )
                .apply("Convert to TransactionRecord", ParDo.of(new DoFn<String, TransactionRecord>() {

                    @ProcessElement
                    public void processElement(ProcessContext c) throws Exception {
                        logger.info("Transaction string is :" + c.element());
                        c.output(Json2ObjectUtils.convertTo(c.element())); // Deliver propagation
                    }
                }));


        txnInput.apply("Manipulate Relationship", MapElements.into(TypeDescriptors.strings()).via(
                (TransactionRecord record) ->
                        Neo4JUtils.manipulateRelationshipFromTransactionRecord(record)
        )).apply(
                "Merge to Neo4j", ParDo.of(new DoFn<String, Void>() {

                    @ProcessElement
                    public void process(ProcessContext c) {
                        logger.info(c.element());

                        try {
                            new Thread(
                                    new Neo4JOpThread(neo4jurl, neo4jUserName, neo4jPassword)
                                            .setLogMsg("Committed")
                                            .setStatement(c.element())
                            ).start();

                        } catch (Exception e) {
                            logger.error(e.getLocalizedMessage());
                        }
                    }
                })
        );

        pipeline.run();

    }


}
