package com.solace.connector.beam.examples;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.solace.connector.beam.SolaceIO;
import com.solace.connector.beam.examples.common.SolaceTextRecord;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * An example that binds to a Solace queue, consumes messages, and then writes them to BigTable.
 *
 * You will need to make sure there is a BigTable table with appropriate schema already created.
 *
 * <p>By default, the examples will run with the {@code DirectRunner}. To run the pipeline on
 * Google Dataflow, specify:
 *
 * <pre>{@code
 * --runner=DataflowRunner
 * }</pre>
 * <p>
 */

public class SolaceBeamBigTable {

    private static final Logger LOG = LoggerFactory.getLogger(SolaceRecordTest.class);

    public interface Options extends DataflowPipelineOptions {

        @Description("IP and port of the client appliance. (e.g. -cip=192.168.160.101)")
        String getCip();

        void setCip(String value);

        @Description("VPN name")
        String getVpn();

        void setVpn(String value);

        @Description("Client username")
        String getCu();

        void setCu(String value);

        @Description("Client password (default '')")
        @Default.String("")
        String getCp();

        void setCp(String value);

        @Description("List of queues for subscribing")
        String getSql();

        void setSql(String value);

        @Description("Enable reading sender timestamp to determine freshness of data")
        @Default.Boolean(false)
        boolean getSts();

        void setSts(boolean value);

        @Description("Enable reading sender sequence number to determine duplication of data")
        @Default.Boolean(false)
        boolean getSmi();

        void setSmi(boolean value);

        @Description("The timeout in milliseconds while try to receive a messages from Solace broker")
        @Default.Integer(100)
        int getTimeout();

        void setTimeout(int timeoutInMillis);

        @Description("The Bigtable project ID, this can be different than your Dataflow project")
        @Default.String("bigtable-project")
        String getBigtableProjectId();

        void setBigtableProjectId(String bigtableProjectId);

        @Description("The Bigtable instance ID")
        @Default.String("bigtable-instance")
        String getBigtableInstanceId();

        void setBigtableInstanceId(String bigtableInstanceId);

        @Description("The Bigtable table ID in the instance.")
        @Default.String("bigtable-table")
        String getBigtableTableId();

        void setBigtableTableId(String bigtableTableId);
    }

    private static void WriteToBigTable(Options options) throws Exception {

        List<String> queues = Arrays.asList(options.getSql().split(","));
        boolean useSenderMsgId = options.getSmi();

        /** Create pipeline **/
        Pipeline pipeline = Pipeline.create(options);

        /** Set Solace connection properties **/
        JCSMPProperties jcsmpProperties = new JCSMPProperties();
        jcsmpProperties.setProperty(JCSMPProperties.HOST, options.getCip());
        jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, options.getVpn());
        jcsmpProperties.setProperty(JCSMPProperties.USERNAME, options.getCu());
        jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, options.getCp());

        /** Create object for BigTable table configuration to be used later to run the pipeline **/
        CloudBigtableTableConfiguration bigtableTableConfig =
                new CloudBigtableTableConfiguration.Builder()
                        .withProjectId(options.getBigtableProjectId())
                        .withInstanceId(options.getBigtableInstanceId())
                        .withTableId(options.getBigtableTableId())
                        .build();

        /* The pipeline consists of three components:
         * 1. Reading message from Solace queue
         * 2. Doing any necessary transformation and creating a BigTable row
         * 3. Writing the row to BigTable
         */
        pipeline.apply(SolaceIO.read(jcsmpProperties, queues, SolaceTextRecord.getCoder(), SolaceTextRecord.getMapper())
                        .withUseSenderTimestamp(options.getSts())
                        .withAdvanceTimeoutInMillis(options.getTimeout()))
                .apply("Map to BigTable row",
                        ParDo.of(
                                new DoFn<SolaceTextRecord, Mutation>() {
                                    @ProcessElement
                                    public void processElement(ProcessContext c) {

                                        String uniqueID = UUID.randomUUID().toString();

                                        Put row = new Put(Bytes.toBytes(uniqueID));

                                        /** Create row that will be written to BigTable **/
                                        row.addColumn(
                                                Bytes.toBytes("stats"),
                                                null,
                                                c.element().getPayload().getBytes(StandardCharsets.UTF_8));
                                        c.output(row);
                                    }
                                }))
                .apply("Write to BigTable",
                        CloudBigtableIO.writeToTable(bigtableTableConfig));

        PipelineResult result = pipeline.run();

        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }

    }

    public static void main(String[] args) {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(SolaceBeamBigTable.Options.class);

        try {
            WriteToBigTable(options);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}