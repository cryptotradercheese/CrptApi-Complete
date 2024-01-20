package org.example;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class CrptApi {
    public static void main(String[] args) {
        RequestsLimiter limiter = new RequestsLimiter(TimeUnit.MINUTES, 10);
        ObjectMapper mapper = new JsonMapper();
        mapper.findAndRegisterModules();
        HttpClient client = HttpClient.newBuilder().build();
        DocumentCreator documentCreator = new DocumentCreator(limiter, mapper, client);

        URI uri = URI.create("https://ismp.crpt.ru/api/v3/lk/documents/create");
        String json = "{\"description\":\n" +
                "{ \"participantInn\": \"string\" }, \"doc_id\": \"string\", \"doc_status\": \"string\",\n" +
                "\"doc_type\": \"LP_INTRODUCE_GOODS\", \"importRequest\": true,\n" +
                "\"owner_inn\": \"string\", \"participant_inn\": \"string\", \"producer_inn\":\n" +
                "\"string\", \"production_date\": \"2020-01-23\", \"production_type\": \"string\",\n" +
                "\"products\": [ { \"certificate_document\": \"string\",\n" +
                "\"certificate_document_date\": \"2020-01-23\",\n" +
                "\"certificate_document_number\": \"string\", \"owner_inn\": \"string\",\n" +
                "\"producer_inn\": \"string\", \"production_date\": \"2020-01-23\",\n" +
                "\"tnved_code\": \"string\", \"uit_code\": \"string\", \"uitu_code\": \"string\" } ],\n" +
                "\"reg_date\": \"2020-01-23\", \"reg_number\": \"string\"}";
        try {
            Document document = mapper.readValue(json, Document.class);
            String signature = "signature";
            System.out.println(document);
            documentCreator.createDocument(uri, document, signature);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * The class is responsible for limiting requests arriving in the application.
     * Class is not synchronized, but it is fine for the current task.
     * A single instance can't be shared between multiple threads.
     */
    public static class RequestsLimiter {
        private TimeUnit timeUnit;
        private int requestLimit;
        private Queue<Long> sentTimes;

        public RequestsLimiter(TimeUnit timeUnit, int requestLimit) {
            Objects.requireNonNull(timeUnit);
            if (requestLimit <= 0) {
                throw new IllegalArgumentException("requestLimit must be positive");
            }

            this.timeUnit = timeUnit;
            this.requestLimit = requestLimit;
            sentTimes = new ArrayDeque<>();
        }

        public boolean add() {
            if (!isFull()) {
                sentTimes.add(currentTimeNanos());
                return true;
            }
            return false;
        }

        private void removeOld() {
            while (!sentTimes.isEmpty()
                    && sentTimes.element() < currentTimeNanos() - timeUnit.toNanos(1)) {
                sentTimes.remove();
            }
        }

        public boolean isFull() {
            removeOld();
            return sentTimes.size() >= requestLimit;
        }

        /**
         * @return unix epoch time in nanoseconds
         * @implNote
         * Using long as a unix epoch time in nanoseconds container makes the method correct
         * for a few hundreds years ahead before overflow
         */
        private long currentTimeNanos() {
            Instant instant = Instant.now();
            return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
        }
    }

    /**
     * All writes happen-before all reads.
     * This class is not used for the current solution,
     * but can be used in the future in case of extension of the functionality.
     */
    public static class ConcurrentRequestsLimiter {
        private TimeUnit timeUnit;
        private int requestLimit;
        private Queue<Long> sentTimes;

        public ConcurrentRequestsLimiter(TimeUnit timeUnit, int requestLimit) {
            Objects.requireNonNull(timeUnit);
            if (requestLimit <= 0) {
                throw new IllegalArgumentException("requestLimit must be positive");
            }

            this.timeUnit = timeUnit;
            this.requestLimit = requestLimit;
            sentTimes = new ArrayDeque<>();
        }

        public synchronized boolean add() {
            if (!isFull()) {
                sentTimes.add(currentTimeNanos());
                return true;
            }
            return false;
        }

        public synchronized boolean isFull() {
            removeOld();
            return sentTimes.size() >= requestLimit;
        }

        private void removeOld() {
            while (!sentTimes.isEmpty()
                    && sentTimes.element() < currentTimeNanos() - timeUnit.toNanos(1)) {
                sentTimes.remove();
            }
        }

        /**
         * @return unix epoch time in nanoseconds
         * @implNote
         * Using long as a unix epoch time in nanoseconds container makes the method correct
         * for a few hundreds years ahead before overflow
         */
        private long currentTimeNanos() {
            Instant instant = Instant.now();
            return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
        }
    }

    /**
     * The class creates documents if the associated {@code RequestsLimiter} allows to,
     * else stores the documents for future creation.
     */
    public static class DocumentCreator {
        private Queue<Document> awaitingDocuments;
        private Queue<String> awaitingSignatures;
        // dependencies
        private RequestsLimiter limiter;
        private ObjectMapper mapper;
        private HttpClient client;

        public DocumentCreator(RequestsLimiter limiter, ObjectMapper mapper, HttpClient client) {
            Objects.requireNonNull(limiter);
            Objects.requireNonNull(mapper);
            Objects.requireNonNull(client);

            awaitingDocuments = new ArrayDeque<>();
            awaitingSignatures = new ArrayDeque<>();
            this.limiter = limiter;
            this.mapper = mapper;
            this.client = client;
        }

        /**
         * @implNote
         * There is no application of {@code signature} specified in the technical task,
         * so the argument is just ignored.
         */
        public synchronized void createDocument(URI uri, Document document, String signature)
                throws IOException, InterruptedException {
            awaitingDocuments.add(document);
            awaitingSignatures.add(signature);

            while (!limiter.isFull() && !awaitingDocuments.isEmpty()) {
                String body = mapper.writeValueAsString(awaitingDocuments.remove());
                awaitingSignatures.remove();
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(uri)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
                HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                limiter.add();
            }
        }
    }

    // DTOs

    public static class Document {
        @JsonProperty
        private Description description;

        @JsonProperty("doc_id")
        private String docId;

        @JsonProperty("doc_status")
        private String docStatus;

        @JsonProperty("doc_type")
        private String docType; // enum

        @JsonProperty
        private boolean importRequest;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("participant_inn")
        private String participantInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        private LocalDate productionDate;

        @JsonProperty("production_type")
        private String productionType;

        @JsonProperty
        private List<Product> products;

        @JsonProperty("reg_date")
        private LocalDate regDate;

        @JsonProperty("reg_number")
        private String regNumber;

        // Getters and setters
        public Description getDescription() {
            return description;
        }

        public void setDescription(Description description) {
            this.description = description;
        }

        public String getDocId() {
            return docId;
        }

        public void setDocId(String docId) {
            this.docId = docId;
        }

        public String getDocStatus() {
            return docStatus;
        }

        public void setDocStatus(String docStatus) {
            this.docStatus = docStatus;
        }

        public String getDocType() {
            return docType;
        }

        public void setDocType(String docType) {
            this.docType = docType;
        }

        public boolean isImportRequest() {
            return importRequest;
        }

        public void setImportRequest(boolean importRequest) {
            this.importRequest = importRequest;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public void setOwnerInn(String ownerInn) {
            this.ownerInn = ownerInn;
        }

        public String getParticipantInn() {
            return participantInn;
        }

        public void setParticipantInn(String participantInn) {
            this.participantInn = participantInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public void setProducerInn(String producerInn) {
            this.producerInn = producerInn;
        }

        public LocalDate getProductionDate() {
            return productionDate;
        }

        public void setProductionDate(LocalDate productionDate) {
            this.productionDate = productionDate;
        }

        public String getProductionType() {
            return productionType;
        }

        public void setProductionType(String productionType) {
            this.productionType = productionType;
        }

        public List<Product> getProducts() {
            return products;
        }

        public void setProducts(List<Product> products) {
            this.products = products;
        }

        public LocalDate getRegDate() {
            return regDate;
        }

        public void setRegDate(LocalDate regDate) {
            this.regDate = regDate;
        }

        public String getRegNumber() {
            return regNumber;
        }

        public void setRegNumber(String regNumber) {
            this.regNumber = regNumber;
        }
    }

    public static class Description {
        @JsonProperty
        private String participantInn;

        // Getters and setters
        public String getParticipantInn() {
            return participantInn;
        }

        public void setParticipantInn(String participantInn) {
            this.participantInn = participantInn;
        }
    }

    public static class Product {
        @JsonProperty("certificate_document")
        private String certificateDocument;

        @JsonProperty("certificate_document_date")
        private LocalDate certificateDocumentDate;

        @JsonProperty("certificate_document_number")
        private String certificateDocumentNumber;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        private LocalDate productionDate;

        @JsonProperty("tnved_code")
        private String tnvedCode;

        @JsonProperty("uit_code")
        private String uitCode;

        @JsonProperty("uitu_code")
        private String uituCode;

        // Getters and setters
        public String getCertificateDocument() {
            return certificateDocument;
        }

        public void setCertificateDocument(String certificateDocument) {
            this.certificateDocument = certificateDocument;
        }

        public LocalDate getCertificateDocumentDate() {
            return certificateDocumentDate;
        }

        public void setCertificateDocumentDate(LocalDate certificateDocumentDate) {
            this.certificateDocumentDate = certificateDocumentDate;
        }

        public String getCertificateDocumentNumber() {
            return certificateDocumentNumber;
        }

        public void setCertificateDocumentNumber(String certificateDocumentNumber) {
            this.certificateDocumentNumber = certificateDocumentNumber;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public void setOwnerInn(String ownerInn) {
            this.ownerInn = ownerInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public void setProducerInn(String producerInn) {
            this.producerInn = producerInn;
        }

        public LocalDate getProductionDate() {
            return productionDate;
        }

        public void setProductionDate(LocalDate productionDate) {
            this.productionDate = productionDate;
        }

        public String getTnvedCode() {
            return tnvedCode;
        }

        public void setTnvedCode(String tnvedCode) {
            this.tnvedCode = tnvedCode;
        }

        public String getUitCode() {
            return uitCode;
        }

        public void setUitCode(String uitCode) {
            this.uitCode = uitCode;
        }

        public String getUituCode() {
            return uituCode;
        }

        public void setUituCode(String uituCode) {
            this.uituCode = uituCode;
        }
    }
}