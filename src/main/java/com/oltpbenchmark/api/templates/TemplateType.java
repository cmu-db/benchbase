/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference
// Implementation, v2.3.0.1
// See <a href="https://javaee.github.io/jaxb-v2/">https://javaee.github.io/jaxb-v2/</a>
// Any modifications to this file will be lost upon recompilation of the source schema.
// Generated on: 2023.11.16 at 08:29:59 AM UTC
//

package com.oltpbenchmark.api.templates;

import jakarta.xml.bind.annotation.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Java class for templateType complex type.
 *
 * <p>The following schema fragment specifies the expected content contained within this class.
 *
 * <pre>
 * &lt;complexType name="templateType"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="query" type="{http://www.w3.org/2001/XMLSchema}string"/&gt;
 *         &lt;element name="types" type="{}typesType"/&gt;
 *         &lt;element name="values" type="{}valuesType" maxOccurs="unbounded"/&gt;
 *       &lt;/sequence&gt;
 *       &lt;attribute name="name" use="required" type="{http://www.w3.org/2001/XMLSchema}string" /&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(
    name = "templateType",
    propOrder = {"query", "types", "values"})
public class TemplateType {

  @XmlElement(required = true)
  protected String query;

  @XmlElement(required = true)
  protected TypesType types;

  @XmlElement(required = true)
  protected List<ValuesType> values;

  @XmlAttribute(name = "name", required = true)
  protected String name;

  /**
   * Gets the value of the query property.
   *
   * @return possible object is {@link String }
   */
  public String getQuery() {
    return this.query;
  }

  /**
   * Sets the value of the query property.
   *
   * @param value allowed object is {@link String }
   */
  public void setQuery(String value) {
    this.query = value;
  }

  /**
   * Gets the value of the types property.
   *
   * @return possible object is {@link TypesType }
   */
  public TypesType getTypes() {
    return this.types;
  }

  /**
   * Sets the value of the types property.
   *
   * @param value allowed object is {@link TypesType }
   */
  public void setTypes(TypesType value) {
    this.types = value;
  }

  /**
   * Gets the value of the values property.
   *
   * <p>This accessor method returns a reference to the live list, not a snapshot. Therefore any
   * modification you make to the returned list will be present inside the JAXB object. This is why
   * there is not a <CODE>set</CODE> method for the values property.
   *
   * <p>For example, to add a new item, do as follows:
   *
   * <pre>
   * getValues().add(newItem);
   * </pre>
   *
   * <p>Objects of the following type(s) are allowed in the list {@link ValuesType }
   */
  public List<ValuesType> getValuesList() {
    if (this.values == null) {
      this.values = new ArrayList<ValuesType>();
    }
    return this.values;
  }

  /**
   * Gets the value of the name property.
   *
   * @return possible object is {@link String }
   */
  public String getName() {
    return this.name;
  }

  /**
   * Sets the value of the name property.
   *
   * @param value allowed object is {@link String }
   */
  public void setName(String value) {
    this.name = value;
  }
}
