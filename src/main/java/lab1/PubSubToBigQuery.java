package lab1;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.apache.beam.vendor.grpc.v1p36p0.com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class PubSubToBigQuery {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubToBigQuery.class);

    private static TupleTag<AccountTableSchema> validMessage = new TupleTag<AccountTableSchema>() {};
    private static TupleTag<String> invalidMessage = new TupleTag<String>(){};

    //setting options
    public interface Options extends DataflowPipelineOptions {
        @Description("BigQuery table name")
        String getTableName();
        void setTableName(String tableName);
        
        @Description("PubSub Subscription")
        String getSubTopic();
        void setSubTopic(String subTopic);

        @Description("Dlq topic")
        String getDlqTopic();
        void setDlqTopic(String dlqTopic);
    }

    // schema to convert JSON to row
    public static final Schema rawSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();


    //main execution of pipeline starts from here
    public static void main(String[] args) {
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        run(options);
    }

    static class JsonToAccount extends DoFn<String,AccountTableSchema> {
        @ProcessElement
        public void processElement(@Element String js,ProcessContext processContext)throws Exception{
            try {
                Gson gson = new Gson();
                AccountTableSchema accountTable = gson.fromJson(js, AccountTableSchema.class);
                processContext.output(validMessage,accountTable);
            }
            catch(Exception e){       //exception if invalid message
                e.printStackTrace();
                processContext.output(invalidMessage,js);
            }
        }
    }

    public static PipelineResult run(Options options) {

        //create pipeline
        Pipeline pipeline = Pipeline.create(options);
        LOG.info("Building pipeline...");

        //Messages read from PubSub subscription and filtered into two types - Valid  and Invalid messages
        PCollectionTuple pubSubMessage = pipeline.apply("Read from PubSub subscription", PubsubIO.readStrings().
                        fromSubscription(options.getSubTopic())).apply("", ParDo.of(new JsonToAccount())
                        .withOutputTags(validMessage, TupleTagList.of(invalidMessage)));

        //Two PCollections for Valid and Invalid message each
        PCollection<AccountTableSchema> validMsg = pubSubMessage.get(validMessage);
        PCollection<String> invalidMsg = pubSubMessage.get(invalidMessage);

        //Write the Valid message to output BigQuery table
        validMsg.apply("Convert Gson to Json",ParDo.of(new DoFn<AccountTableSchema, String>() {
                    @ProcessElement
                    public void convert(ProcessContext context){
                        Gson gs = new Gson();
                        String gsString = gs.toJson(context.element());
                        context.output(gsString);
                    }}))

                //convert from Json to Row using rawSchema
                    .apply("Convert from json to row",JsonToRow.withSchema(rawSchema))
                    .apply("Write to output BigQuery table",BigQueryIO.<Row>write().to(options.getTableName())
                            .useBeamSchema().withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                            .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        //Write the Incorrect message type to Dlq
        invalidMsg.apply("Invalid Message To DLQ",PubsubIO.writeStrings().to(options.getDlqTopic()));

        return pipeline.run();

    }

}
