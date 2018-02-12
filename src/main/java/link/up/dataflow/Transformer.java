package link.up.dataflow;

import link.up.dataflow.entity.CreditCard;
import link.up.dataflow.entity.Customer;
import link.up.dataflow.entity.TransactionRecord;
import link.up.dataflow.utils.CreatorUtils;
import link.up.dataflow.utils.Json2ObjectUtils;
import link.up.dataflow.utils.Neo4JOpThread;
import link.up.dataflow.utils.Neo4JUtils;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

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

        String getBucket();

        void setBucket(String bucket);

    }

    public static void main(String args[]) {

        String projectId = args[0].split("=")[1];

        String topicName = TOPIC_NAME_TEMPLATE.replace("PROJECT_NAME", projectId)
                .replace("TOPIC_NAME", args[1].split("=")[1]);

        MyOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        myOptions.setStreaming(true);
        myOptions.setJobName("Pubsub-Linkup-demo");

        String bucket = myOptions.getBucket();

        String neo4jurl = myOptions.getNeo4jUrl();
        String neo4jUserName = myOptions.getUserName();
        String neo4jPassword = myOptions.getPassword();


        /**
         *  Step #1 - Create PIPELINE
         */
        Pipeline pipeline = Pipeline.create(myOptions);


        /**
         * Step #2 - Read from GCS for CUSTOMER & ACCOUNT_ASSOC info and convert to VIEW(map)
         */

        /**
         *  Key  : Customer ID
         *  Value: Customer Object
         */

        PCollectionView<Map<String, Customer>> customerView =
                pipeline.apply("Read Customer Data", TextIO.read().from("gs://" + bucket + "/customer_info_dataset.csv"))
                        .apply(
                                ParDo.of(new DoFn<String, KV<String, Customer>>() {

                                    @ProcessElement
                                    public void process(ProcessContext context) {

                                        String line = context.element();
                                        String[] cells = line.split(",");
                                        KV kv = KV.of(cells[0], CreatorUtils.createCustomer(line));
                                        context.output(kv);
                                    }
                                })
                        ).apply(View.<String, Customer>asMap());


        /**
         *  Key  : Account Number
         *  Value: Credit Card Object
         */
        PCollectionView<Map<String, CreditCard>> creditCardView =
                pipeline.apply("Read CreditCard Data", TextIO.read().from("gs://" + bucket + "/account_assoc_info_dataset.csv"))
                        .apply(
                                ParDo.of(new DoFn<String, KV<String, CreditCard>>() {

                                    @ProcessElement
                                    public void process(ProcessContext context) {

                                        String line = context.element();
                                        String[] cells = line.split(",");
                                        KV kv = KV.of(cells[0], CreatorUtils.createCreditCard(line));
                                        context.output(kv);
                                    }
                                })
                        ).apply(View.<String, CreditCard>asMap());


        /**
         *  Step #3 - Read from PubSub
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


        /**
         *   Enrich CUSTOMER / ACCOUNT info for Neo4j nodes
         *
         *   Custom A / Account A is TRANSFER_FROM
         *   Custom B / Account B is TRANSFER_TO
         */

        txnInput.apply("Manipulate Relationship", ParDo.of(new DoFn<TransactionRecord, String>() {
            @ProcessElement
            public void process(ProcessContext context) {

                Map<String, CreditCard> ccMap = context.sideInput(creditCardView);
                Map<String, Customer> custMap = context.sideInput(customerView);

                TransactionRecord txn = context.element();

                CreditCard ccA = ccMap.get(txn.getNameOrig());
                CreditCard ccB = ccMap.get(txn.getNameDest());


                context.output(Neo4JUtils.manipulateRelationshipFromTransactionRecord(
                        txn,
                        custMap.get(ccA.getCustomerId()),
                        ccA,
                        custMap.get(ccB.getCustomerId()),
                        ccB));
            }
        }).withSideInputs(customerView, creditCardView))

                // Execute MERGE to NEO4J
                .apply(
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
