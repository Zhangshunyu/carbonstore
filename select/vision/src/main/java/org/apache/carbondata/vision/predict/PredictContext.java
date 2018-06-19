/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.vision.predict;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.carbondata.vision.algorithm.Algorithm;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.model.Model;
import org.apache.carbondata.vision.table.Table;

import org.apache.hadoop.io.Writable;

public class PredictContext implements Serializable, Writable {

  private Algorithm algorithm;
  private Model model;
  private Table table;
  private VisionConfiguration conf;

  public PredictContext() {
    this.algorithm = new Algorithm();
    this.model = new Model();
    this.table = new Table();
    this.conf = new VisionConfiguration();
  }

  public PredictContext(Algorithm algorithm, Model model, Table table, VisionConfiguration conf) {
    this.algorithm = algorithm;
    this.model = model;
    this.table = table;
    this.conf = conf;
  }

  public Algorithm getAlgorithm() {
    return algorithm;
  }

  public void setAlgorithm(Algorithm algorithm) {
    this.algorithm = algorithm;
  }

  public Model getModel() {
    return model;
  }

  public void setModel(Model model) {
    this.model = model;
  }

  public Table getTable() {
    return table;
  }

  public void setTable(Table table) {
    this.table = table;
  }

  public VisionConfiguration getConf() {
    return conf;
  }

  public void setConf(VisionConfiguration conf) {
    this.conf = conf;
  }

  @Override

  public void write(DataOutput out) throws IOException {
    algorithm.write(out);
    model.write(out);
    table.write(out);
    conf.write(out);
  }

  @Override public void readFields(DataInput in) throws IOException {
    algorithm.readFields(in);
    model.readFields(in);
    table.readFields(in);
    conf.readFields(in);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Algorithm algorithm;
    private Model model;
    private Table table;
    private VisionConfiguration conf;

    Builder() {
      conf = new VisionConfiguration();
    }

    public Builder model(Model model) {
      this.model = model;
      return this;
    }

    public Builder algorithm(Algorithm algorithm) {
      this.algorithm = algorithm;
      return this;
    }

    public Builder table(Table table) {
      this.table = table;
      return this;
    }

    public Builder conf(String key, Object value) {
      conf.conf(key, value);
      return this;
    }

    public Builder conf(VisionConfiguration conf) {
      this.conf.conf(conf);
      return this;
    }

    public PredictContext create() {
      return new PredictContext(algorithm, model, table, conf);
    }
  }
}
