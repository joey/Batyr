package com.cloudera.batyr.mapreduce;

import com.cloudera.batyr.mapreduce.Format;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface FileOutput {

  Format value();
}
