package com.bsolz.kafka.libraryeventsconsumer.repositories;

import com.bsolz.kafka.libraryeventsconsumer.entities.FailureRecord;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface FailureRecordRepository extends JpaRepository<FailureRecord, Integer> {
    List<FailureRecord> findAllByStatus(String retry);
}
