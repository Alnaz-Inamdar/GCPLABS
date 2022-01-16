package org.example;

import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import com.google.gson.JsonSyntaxException;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class pubSubToBq {

    private static final Logger LOG = (Logger) LoggerFactory.getLogger(pubSubToBq.class);

    static final TupleTag<CommonLog> parsedMessages = new TupleTag<CommonLog>() {};
    static final TupleTag<String> unparsedMessages = new TupleTag<String>() {};

    public interface myOptions extends DataflowPipelineOptions{

        @Description("Input topic name")
        String getInputTopic();
        void setInputTopic(String inputTopic);

        @Description("Subscription name")
        String getsubTopic();
        void setsubTopic(String subscription);

        @Description("Output BqTable name")
        String getTableName();
        void setTableName(String tableName);

        @Description("DlqTopic name")
        String getDlqTopic();
        void setDlqTopic(String dlqTopic);

        @Description("DlqSubscription name")
        String getDlqSubscription();
        void setDlqSubscription(String dlqSubscription);

        @Description("Window allowed lateness, in days")
        Integer getAllowedLateness();
        void setAllowedLateness(Integer allowedLateness);
    }

    public static class PubsubMessageToCommonLog extends PTransform<PCollection<String>, PCollectionTuple> {
        @Override
        public PCollectionTuple expand(PCollection<String> input) {
            return input
                    .apply("JsonToCommonLog", ParDo.of(new DoFn<String, CommonLog>() {
                                @ProcessElement
                                public void processElement(ProcessContext context) {
                                    String json = context.element();
                                    Gson gson = new Gson();
                                    try {
                                        CommonLog commonLog = gson.fromJson(json, CommonLog.class);
                                        context.output(parsedMessages, commonLog);
                                    } catch (JsonSyntaxException e) {
                                        context.output(unparsedMessages, json);
                                    }

                                }
                            })
                            .withOutputTags(parsedMessages, TupleTagList.of(unparsedMessages)));
        }
    }

    public static final Schema rawSchema = Schema
            .builder()
            .addInt64Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();

    public static void main(String[] args){
        PipelineOptionsFactory.register(myOptions.class);
        myOptions myoptions = PipelineOptionsFactory.fromArgs(args)
                .withValidation()
                .as(myOptions.class);
        run(myoptions);
    }

    public static PipelineResult run(myOptions myOptions){
        Pipeline pipeline = Pipeline.create(myOptions);
        LOG.info("Building pipeline...");

        PCollectionTuple transformOut =
                pipeline.apply("ReadPubSubMessages", PubsubIO.readStrings()
                                .withTimestampAttribute("timestamp")
                                .fromSubscription(myOptions.getsubTopic()))
                        .apply("ConvertMessageToCommonLog", new PubsubMessageToCommonLog());

        transformOut
                // Retrieve parsed messages
                .get(parsedMessages)
                .apply("WindowByMinute", Window.<CommonLog>into(
                                FixedWindows.of(Duration.standardSeconds(50)))
                        .withAllowedLateness(
                                Duration.standardDays(myOptions.getAllowedLateness()))
                        .triggering(AfterWatermark.pastEndOfWindow()
                                .withLateFirings(AfterPane.elementCountAtLeast(1)))
                        .accumulatingFiredPanes())
                // update to Group.globally() after resolved: https://issues.apache.org/jira/browse/BEAM-10297
                // Only if supports Row output
                .apply("CountPerMinute", Combine.globally(Count.<CommonLog>combineFn())
                        .withoutDefaults())
                .apply("ConvertToRow", ParDo.of(new DoFn<Long, Row>() {
                    @ProcessElement
                    public void processElement(@Element Long views, OutputReceiver<Row> r, IntervalWindow window) {
                        Instant i = Instant.ofEpochMilli(window.end()
                                .getMillis());
                        Row row = Row.withSchema(rawSchema)
                                .addValues(views, i)
                                .build();
                        r.output(row);
                    }
                }))
                .setRowSchema(rawSchema)
                .apply("WriteToBQ", BigQueryIO.<Row>write().to(myOptions.getTableName())
                        .useBeamSchema()
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));


        return pipeline.run();
    }

}
