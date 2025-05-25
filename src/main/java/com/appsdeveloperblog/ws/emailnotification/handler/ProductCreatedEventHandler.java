package com.appsdeveloperblog.ws.emailnotification.handler;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.logging.Logger;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.emailnotification.error.NotRetryableException;
import com.appsdeveloperblog.ws.emailnotification.error.RetryableException;
import com.appsdeveloperblog.ws.emailnotification.io.ProcessedEventEntity;
import com.appsdeveloperblog.ws.emailnotification.repository.ProcessedEventRepository;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private Logger logger = Logger.getLogger(ProductCreatedEventHandler.class.getName());

    private RestTemplate restTemplate;
    @Autowired
    private ProcessedEventRepository processedEventRepository;

    public ProductCreatedEventHandler(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @KafkaHandler
    public void handle(@Payload ProductCreatedEvent event,
            @Header(value = "messageId", required = true) String messageId,
            @Header(KafkaHeaders.RECEIVED_KEY) String messageKey) {

        if (processedEventRepository.findByMessageId(messageId) != null) {
            logger.info("Event with messageId: " + messageId + " has already been processed. Skipping.");
            return;
        }
        // Logic to handle the product created event
        logger.info("Received product created event: " + event.getTitle() + " with ID: " + event.getProductId()
                + " and price: " + event.getPrice() + " and quantity: " + event.getQuantity());

        String theUrl = "http://localhost:8082/response/200";
        try {
            ResponseEntity<String> response = restTemplate.exchange(theUrl, HttpMethod.GET, null, String.class);
            if (response.getStatusCode().is2xxSuccessful()) {
                logger.info("Successfully called the external service: " + response.getBody());
            } else {
                logger.warning("Failed to call the external service, status code: " + response.getStatusCode());
                throw new NotRetryableException("Failed to call the external service");
            }
        } catch (ResourceAccessException ex) {
            logger.severe("Invalid URL: " + theUrl + " - " + ex.getMessage());
            throw new RetryableException("Invalid URL: " + theUrl + " - " + ex.getMessage());
        } catch (Exception ex) {
            logger.severe("Exception while calling external service: " + ex.getMessage());
            throw new NotRetryableException("Exception while calling external service" + ex.getMessage());
        }
        try {
            processedEventRepository.save(new ProcessedEventEntity(messageId, event.getProductId()));
            logger.info("Processed event with messageId: " + messageId + " and productId: " + event.getProductId());
        } catch (DataIntegrityViolationException ex) {
            logger.severe("DatiIntegrationViolation occurred: " + ex.getMessage());
            throw new NotRetryableException("DataIntegrityViolationException occurred: " + ex.getMessage());
        }
    }

}
