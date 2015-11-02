/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.table

/**
 * This is used for executing Table API operations. We use manually generated
 * TypeInfo to check the field types and create serializers and comparators.
 */
class Row(arity: Int) extends Product {

  private val fields = new Array[Any](arity)

  def this(fields: Any*) = {
    this(fields.length)
    fields.copyToArray(this.fields)
  }

  def this(copyFrom: Row, fields: Any*) = {
    this(copyFrom.productArity + fields.size)
    copyFrom.fields.copyToArray(this.fields)
    fields.copyToArray(this.fields, copyFrom.productArity)
  }

  def productArity = fields.length

  def productElement(i: Int): Any = fields(i)

  def canEqual(that: Any) = false

  def apply(i: Int): Any = productElement(i)

  def apply(fieldName: String)(implicit s: Schema): Any =
    productElement(s.index(fieldName))

  def getAs[T](i: Int): T =
    productElement(i).asInstanceOf[T]

  def getAs[T](fieldName: String)(implicit s: Schema): T =
    getAs[T](s.index(fieldName))

  def setField(i: Int, value: Any): Unit =
    fields(i) = value

  def setField(fieldName: String, value: Any)(implicit s: Schema): Unit =
    fields(s.index(fieldName)) = value

  def update(i: Int, value: Any): Unit =
    fields(i) = value

  def update(fieldName: String, value: Any)(implicit s: Schema): Unit =
    fields(s.index(fieldName)) = value

  override def toString = fields.mkString(",")

}
