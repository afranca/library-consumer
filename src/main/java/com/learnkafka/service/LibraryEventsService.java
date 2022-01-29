package com.learnkafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafka.entity.LibraryEvent;
import com.learnkafka.jpa.LibraryEventsRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
@Slf4j
public class LibraryEventsService {

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    public void processLibraryEvent(ConsumerRecord<Integer,String> consumerRecord) throws JsonProcessingException {

        LibraryEvent libraryEvent = objectMapper.readValue(consumerRecord.value(), LibraryEvent.class);
        switch (libraryEvent.getLibraryEventType()){
            case NEW:
                log.info("NEW : {}",consumerRecord);
                save(libraryEvent);
                break;
            case UPDATE:
                log.info("UPDATE : {}",consumerRecord);
                validate(libraryEvent);
                save(libraryEvent);
                break;

            default:
                throw new IllegalStateException("Unexpected value: " + libraryEvent.getLibraryEventType());
        }

        //log.info("ConsumerRecord : {}",consumerRecord);

    }

    private void validate(LibraryEvent libraryEvent) {
        if (libraryEvent.getLibraryEventId()==null){
            throw new IllegalArgumentException("Library Event Id is missing");
        }

        Optional<LibraryEvent> libraryEventOptional  = libraryEventsRepository.findById(libraryEvent.getLibraryEventId());
        if (!libraryEventOptional.isPresent()){
            throw new IllegalArgumentException("Not a valid library Event");
        }
        log.info("Validation Successful for Library Event : {}",libraryEventOptional.get());

    }

    private void save(LibraryEvent libraryEvent) {
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);
        log.info("Successfully Persisted the library Event {}", libraryEvent);
    }
}
