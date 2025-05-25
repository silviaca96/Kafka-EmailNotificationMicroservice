package com.appsdeveloperblog.ws.emailnotification.handler;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.logging.Logger;

import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestTemplate;

import com.appsdeveloperblog.ws.core.ProductCreatedEvent;
import com.appsdeveloperblog.ws.emailnotification.error.NotRetryableException;
import com.appsdeveloperblog.ws.emailnotification.error.RetryableException;

@Component
@KafkaListener(topics = "product-created-events-topic")
public class ProductCreatedEventHandler {

    private Logger logger = Logger.getLogger(ProductCreatedEventHandler.class.getName());

    private RestTemplate restTemplate;

    public ProductCreatedEventHandler(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @KafkaHandler
    public void handle(ProductCreatedEvent event) {
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
       
    }
}
