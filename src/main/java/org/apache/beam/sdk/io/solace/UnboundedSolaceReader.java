package org.apache.beam.sdk.io.solace;

import com.google.common.annotations.VisibleForTesting;
import com.solacesystems.jcsmp.*;
import org.apache.beam.sdk.io.UnboundedSource;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPath;
import java.io.ByteArrayInputStream;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unbounded Reader to read messages from a Solace Router.
 */
@VisibleForTesting
class UnboundedSolaceReader<T> extends UnboundedSource.UnboundedReader<T> {
    private static final Logger LOG = LoggerFactory.getLogger(UnboundedSolaceReader.class);

    // The closed state of this {@link UnboundedSolaceReader}. If true, the reader has not yet been closed,
    private AtomicBoolean active = new AtomicBoolean(true);

    private final UnboundedSolaceSource<T> source;
    private JCSMPSession session;
    private XMLMessageProducer prod;
    private FlowReceiver flowReceiver;
    private boolean isAutoAck;
    private String clientName;
    private Topic SEMPTopic;
    private String SEMPVersion;

    private T current;
    private Instant currentTimestamp;

    /**
     * Queue to palce advanced messages before {@link #getCheckpointMark()} be called
     * non concurrent queue, should only be accessed by the reader thread
     * A given {@link UnboundedReader} object will only be accessed by a single thread at once.
     */
    private java.util.Queue<BytesXMLMessage> wait4cpQueue = new LinkedList<BytesXMLMessage>();

    /**
     * Queue to place messages ready to ack, will be accessed by {@link #getCheckpointMark()}
     * and {@link SolaceCheckpointMark#finalizeCheckpoint()} in defferent thread
     */
    private BlockingQueue<BytesXMLMessage> safe2ackQueue = new LinkedBlockingQueue<BytesXMLMessage>();

    public class PrintingPubCallback implements JCSMPStreamingPublishEventHandler {
        public void handleError(String messageID, JCSMPException cause, long timestamp) {
                LOG.error("Error occurred for Solace queue depth request message: " + messageID);
                cause.printStackTrace();
        }

        // This method is only invoked for persistent and non-persistent
        // messages.
        public void responseReceived(String messageID) {
            LOG.error("Unexpected response to Solace queue depth request message: " + messageID);
        }
}

    public UnboundedSolaceReader(UnboundedSolaceSource<T> source) {
        this.source = source;
        this.current = null;
        SEMPVersion = "soltr/8_13";
    }

    @Override
    public boolean start() throws IOException {
        LOG.info("Starting UnboundSolaceReader for Solace queue: {} ...", source.getQueueName());
        SolaceIO.Read<T> spec = source.getSpec();
        try {
            SolaceIO.ConnectionConfiguration cc = source.getSpec().connectionConfiguration();
            final JCSMPProperties properties = new JCSMPProperties();
            properties.setProperty(JCSMPProperties.HOST, cc.getHost());     // host:port
            properties.setProperty(JCSMPProperties.USERNAME, cc.getUsername()); // client-username
            properties.setProperty(JCSMPProperties.PASSWORD, cc.getPassword()); // client-password
            properties.setProperty(JCSMPProperties.VPN_NAME,  cc.getVpn()); // message-vpn

            if (cc.getClientName() != null) {
                properties.setProperty(JCSMPProperties.CLIENT_NAME,  cc.getClientName()); // message-vpn
            }

            session = JCSMPFactory.onlyInstance().createSession(properties);
            prod = session.getMessageProducer(new PrintingPubCallback());
            clientName = (String)session.getProperty(JCSMPProperties.CLIENT_NAME);
            session.connect();

            String routerName = (String) session.getCapability(CapabilityType.PEER_ROUTER_NAME);
			final String SEMP_TOPIC_STRING = String.format("#SEMP/%s/SHOW", routerName);
			SEMPTopic = JCSMPFactory.onlyInstance().createTopic(SEMP_TOPIC_STRING);

            // do NOT provision the queue, so "Unknown Queue" exception will be threw if the
            // queue is not existed already
            final Queue queue = JCSMPFactory.onlyInstance().createQueue(source.getQueueName());

            // Create a Flow be able to bind to and consume messages from the Queue.
            final ConsumerFlowProperties flow_prop = new ConsumerFlowProperties();
            flow_prop.setEndpoint(queue);

            isAutoAck = spec.connectionConfiguration().isAutoAck();
            if (isAutoAck){
                // auto ack the messages
                flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_AUTO);
            } else {
                // will ack the messages in checkpoint
                flow_prop.setAckMode(JCSMPProperties.SUPPORTED_MESSAGE_ACK_CLIENT);
            }


            EndpointProperties endpoint_props = new EndpointProperties();
            endpoint_props.setAccessType(EndpointProperties.ACCESSTYPE_EXCLUSIVE);
            // bind to the queue, passing null as message listener for no async callback
            flowReceiver = session.createFlow(null, flow_prop, endpoint_props);
            // Start the consumer
            flowReceiver.start();
            LOG.info("Starting Solace session [{}] on queue[{}]..."
                    , clientName
                    , source.getQueueName()
                    );

            return advance();

        } catch (Exception e){
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    @Override
    public boolean advance() throws IOException {
     //   LOG.debug("Advancing Solace session [{}] on queue[{}]..."
     //       , clientName
     //       , source.getQueueName()
     //   );
        SolaceIO.ConnectionConfiguration cc = source.getSpec().connectionConfiguration();
        try {
            BytesXMLMessage msg = flowReceiver.receive(cc.getTimeoutInMillis());
            if (msg == null) {
                return false;
            }

            current = this.source.getSpec().messageMapper().mapMessage(msg);

            // TODO: get sender timestamp
            currentTimestamp = Instant.now();
            
            // onlie client ack mode need to ack message
            if(!isAutoAck){
                wait4cpQueue.add(msg);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        return true;
    }

    @Override
    public void close() throws IOException {
        LOG.info("Close the Solace session [{}] on queue[{}]..."
                , clientName
                , source.getQueueName()
        );
        active.set(false);
        try {
            if (flowReceiver != null) {
                flowReceiver.close();
            }
            if (session != null){
                session.closeSession();
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
        // TODO: add finally block to close session.
    }

    /**
     * Direct Runner will call this method on every second
     */
    @Override
    public Instant getWatermark() {
        if (current == null){
            return Instant.now();
        }
        return currentTimestamp;
    }

    @Override
    public UnboundedSource.CheckpointMark getCheckpointMark() {
        // put all messages in wait4cp to safe2ack
        // and clean the wait4cp queue in the same time
        try {
            BytesXMLMessage msg = wait4cpQueue.poll();
            while(msg != null){
                safe2ackQueue.put(msg);
                msg = wait4cpQueue.poll();
            } 
        }catch(Exception e){
            LOG.error("Got exception while putting into the blocking queue: {}", e);
        }

         SolaceCheckpointMark scm = new SolaceCheckpointMark(
            this,
            clientName);

        return scm;
    }

    /**
     * Ack all message in the safe2ackQueue
     * called by {@link SolaceCheckpointMark #finalizeCheckpoint()}
     * It's possible for a checkpoint to be taken but never finalized
     * So we simply ack all messages which are read to be ack
     */
    public void ackMessages() throws IOException{
        LOG.debug("try to ack {} messages with {} Session [{}]"
            , safe2ackQueue.size()
            , active.get() ? "active" : "closed"
            , clientName);

        if (!active.get()){
            return;
        }
        try {
            while(safe2ackQueue.size()>0){
                BytesXMLMessage msg = safe2ackQueue.poll(0, TimeUnit.NANOSECONDS);
                if (msg != null) {
                    msg.ackMessage();
                }
            } 
        }catch(Exception e){
            LOG.error("Got exception while acking the message: {}", e);
            throw new IOException(e);
        }
    }

    @Override
    public T getCurrent() {
        if (current == null) {
            throw new NoSuchElementException();
        }
        return current;
    }

    @Override
    public Instant getCurrentTimestamp() {
        if (current == null) {
            throw new NoSuchElementException();
        }
        return currentTimestamp;
    }

    @Override
    public UnboundedSolaceSource<T> getCurrentSource() {
        return source;
    }

    public long queryQueueBytes (String queueName, String VPNName) {
        long queueBytes = 0;
        String SEMP_SHOW_QUEUE = "<rpc semp-version=\"" + SEMPVersion + "\">";
        SEMP_SHOW_QUEUE +="<show><queue><name>" + queueName + "</name>";
		SEMP_SHOW_QUEUE +="<vpn-name>" + VPNName + "</vpn-name></queue></show></rpc>";
        try {
            Requestor requestor = session.createRequestor();
            BytesXMLMessage requestMsg = JCSMPFactory.onlyInstance().createMessage(BytesXMLMessage.class);
            requestMsg.writeAttachment(SEMP_SHOW_QUEUE.getBytes());
            BytesXMLMessage replyMsg = requestor.request(requestMsg, 5000, SEMPTopic);
            ByteArrayInputStream input;
            byte[] bytes = new byte[replyMsg.getAttachmentContentLength()];
            replyMsg.readAttachmentBytes(bytes);
            input =  new ByteArrayInputStream(bytes);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document doc = builder.parse(input);
            XPath xPath =  XPathFactory.newInstance().newXPath();
            String expression = "/rpc-reply/rpc/show/queue/queues/queue/info/num-messages-spooled";
            Node node = (Node) xPath.compile(expression).evaluate(
                doc, XPathConstants.NODE);
            queueBytes=Long.parseLong(node.getTextContent());
        } catch (JCSMPException ex ) {
            LOG.error("Encountered a JCSMPException querying queue depth: {}", ex.getMessage());
            return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
        } catch (Exception ex) {
            LOG.error("Encountered a Parser Exception querying queue depth: {}", ex.toString());
            return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;           
        }
        return queueBytes;
    }

    @Override
    public long getSplitBacklogBytes() {
      LOG.debug("Enter getSplitBacklogBytes()");
      long backlogBytes = 0;
      long queuBacklog = queryQueueBytes(source.getQueueName(), source.getVPNName());
      if (queuBacklog == UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN) {
        LOG.error("getSplitBacklogBytes() unable to read bytes from: {}", 
            source.getQueueName());
        return UnboundedSource.UnboundedReader.BACKLOG_UNKNOWN;
      }
      LOG.debug("getSplitBacklogBytes() Reporting backlog bytes of: {} from queue {}", 
        Long.toString(backlogBytes), 
        source.getQueueName());
      return backlogBytes;
    }
}
