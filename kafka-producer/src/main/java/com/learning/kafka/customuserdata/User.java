package com.learning.kafka.customuserdata;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Random;

public record User(@JsonProperty  String user, @JsonProperty int age){
    public User() {
        this("TestUser-->" + new Random().nextInt(), 12);
    }
}