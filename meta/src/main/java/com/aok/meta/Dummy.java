package com.aok.meta;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.Data;

@Data
@JsonTypeName("dummy")
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dummy extends Meta {
}
