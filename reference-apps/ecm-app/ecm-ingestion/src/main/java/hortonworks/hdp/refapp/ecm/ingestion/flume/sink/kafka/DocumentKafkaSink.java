/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package hortonworks.hdp.refapp.ecm.ingestion.flume.sink.kafka;

import hortonworks.hdp.refapp.ecm.ingestion.flume.Constants;
import hortonworks.hdp.refapp.ecm.service.api.DocumentClass;
import hortonworks.hdp.refapp.ecm.service.api.DocumentMetaData;
import hortonworks.hdp.refapp.ecm.service.api.DocumentUpdateRequest;

import java.util.Map;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;
import org.mortbay.log.Log;


/**
 * A Sink that sends new new Document Requests to Kafka Topic
 * @author gvetticaden
 *
 */
public class DocumentKafkaSink extends AbstractSink implements Configurable {

    private static final Logger logger = Logger.getLogger(DocumentKafkaSink.class);
    private Properties producerProps;
    private Producer<String, DocumentUpdateRequest> producer;
    private String topic;
    private Context context;

    @Override
    public Status process() throws EventDeliveryException {
        Status result = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        Event event = null;
        String eventTopic = topic;

        try {
            transaction.begin();
            event = channel.take();

            if (event != null) {
            	DocumentUpdateRequest docUpdateRequest = constructDocumentUpdateRequest(event);
            	KeyedMessage<String, DocumentUpdateRequest> message = new KeyedMessage<String, DocumentUpdateRequest>(eventTopic, docUpdateRequest);
            	if(logger.isInfoEnabled()) {
            		Log.info("Sending docUpdateRequest["+ docUpdateRequest + "] to Kafka topic["+topic + "]");
            	}
            	producer.send(message);
            } else {
                // No event found, request back-off semantics from the sink runner
                result = Status.BACKOFF;
            }
            // publishing is successful. Commit.
            transaction.commit();

        } catch (Exception ex) {
            transaction.rollback();
            String errorMsg = "Failed to publish event: " + event;
            logger.error(errorMsg, ex);
            throw new EventDeliveryException(errorMsg, ex);

        } finally {
            transaction.close();
        }

        return result;
    }
    
	private DocumentUpdateRequest constructDocumentUpdateRequest(Event event) throws Exception {
		
		//the document
		byte[] document = event.getBody();
		Log.info("The document in bytes is: " + document);
		
		//the doc metadata
		DocumentMetaData docMetaData = createDocMetadataFromEvent(event);
		DocumentUpdateRequest docUpdateRequest = new DocumentUpdateRequest(null, document, docMetaData);

		return docUpdateRequest;
	}	

	private DocumentMetaData createDocMetadataFromEvent(Event event) {
		Map<String, String> headers = event.getHeaders();
		String customerName = headers.get(Constants.HEADER_KEY_DOC_CUST_NAME);
		String documentName = headers.get(Constants.HEADER_KEY_DOC_NAME);
		String mimeType = headers.get(Constants.HEADER_KEY_MIME_TYPE);
		String extension = headers.get(Constants.HEADER_KEY_DOC_EXTENSION);
		String docClassTypeString = headers.get(Constants.HEADER_KEY_DOC_CLASS_TYPE);
		
		if(logger.isInfoEnabled()) {
			logger.info("Documsent for Customer["+customerName + "]");;
			logger.info("Document Name is: " + documentName);
			logger.info("Mime Type of doc is: " + mimeType);
			logger.info("Doc extension is: " + extension);
			logger.info("Doc Class Type is: " + docClassTypeString);
		}
		
		//default documentClass to RFP
		DocumentClass docClassType = DocumentClass.RFP;
		if(StringUtils.isNotEmpty(docClassTypeString)) {
			docClassType = DocumentClass.valueOf(docClassTypeString);
		}
		
		DocumentMetaData docMeta = new DocumentMetaData(documentName, mimeType, docClassType, customerName, extension);
		return docMeta;
	}    

    @Override
    public synchronized void start() {
        // instantiate the producer
        ProducerConfig config = new ProducerConfig(producerProps);
        producer = new Producer<String, DocumentUpdateRequest>(config);
        super.start();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        super.stop();
    }


    @Override
    public void configure(Context context) {
        this.context = context;
        // read the properties for Kafka Producer
        // any property that has the prefix "kafka" in the key will be considered as a property that is passed when
        // instantiating the producer.
        // For example, kafka.metadata.broker.list = localhost:9092 is a property that is processed here, but not
        // sinks.k1.type = com.thilinamb.flume.sink.KafkaSink.
        Map<String, String> params = context.getParameters();
        producerProps = new Properties();
        for (String key : params.keySet()) {
            String value = params.get(key).trim();
            key = key.trim();
            if (key.startsWith(Constants.PROPERTY_PREFIX)) {
                // remove the prefix
                key = key.substring(Constants.PROPERTY_PREFIX.length() + 1, key.length());
                producerProps.put(key.trim(), value);
                if (logger.isDebugEnabled()) {
                    logger.debug("Reading a Kafka Producer Property: key: " + key + ", value: " + value);
                }
            }
        }

        // Get the Topic from the config
        topic = context.getString(Constants.TOPIC, Constants.DEFAULT_TOPIC);
        if (topic.equals(Constants.DEFAULT_TOPIC)) {
            logger.warn("The Properties 'metadata.extractor' or 'topic' is not set. Using the default topic name" +
                    Constants.DEFAULT_TOPIC);
        } else {
            logger.info("Using the static topic: " + topic);
        }
    }
}
