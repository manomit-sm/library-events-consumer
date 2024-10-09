package com.bsolz.kafka.libraryeventsconsumer.repositories;

import com.bsolz.kafka.libraryeventsconsumer.entities.LibraryEvent;
import org.springframework.data.jpa.repository.JpaRepository;

public interface LibraryEventRepository extends JpaRepository<LibraryEvent, Integer> {
}
