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

package org.apache.wayang.api

import org.apache.wayang.basic.data.{Tuple2 => WayangTuple2}
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction
import org.apache.wayang.core.function.TransformationDescriptor
import org.apache.wayang.spatial.data.WGeometry
import org.apache.wayang.spatial.function.SpatialPredicate
import org.apache.wayang.spatial.operators.{SpatialFilterOperator, SpatialJoinOperator}

import scala.reflect.ClassTag

/**
 * Provides spatial extensions for DataQuanta via implicit conversions.
 * Import this object to add spatialFilter and spatialJoin methods to DataQuanta (Scala API).
 *
 * For DataQuantaBuilder (Java API), use the built-in spatialFilter/spatialJoin methods
 * with SpatialPredicateType from org.apache.wayang.core.api.spatial.
 *
 * Usage (Scala DataQuanta API):
 * {{{
 * import org.apache.wayang.api.SpatialDataQuantaImplicits._
 *
 * dataQuanta.spatialFilter(keySelector, SpatialPredicate.INTERSECTS, filterGeometry, "geom_column")
 * dataQuanta.spatialJoin(thisKeyUdf, otherDataQuanta, thatKeyUdf, SpatialPredicate.WITHIN)
 * }}}
 *
 * Usage (Java/Scala DataQuantaBuilder API):
 * {{{
 * import org.apache.wayang.core.api.spatial.SpatialPredicateType
 *
 * builder.spatialFilter(keyUdf, SpatialPredicateType.INTERSECTS, filterGeometry)
 * builder.spatialJoin(thisKeyUdf, thatBuilder, thatKeyUdf, SpatialPredicateType.WITHIN)
 * }}}
 */
object SpatialDataQuantaImplicits {

  /**
   * Implicit class to add spatial methods to DataQuanta.
   */
  implicit class SpatialDataQuanta[Out: ClassTag](val self: DataQuanta[Out]) {

    def spatialFilter(keySelector: SerializableFunction[Out, WGeometry],
                      spatialPredicate: SpatialPredicate,
                      filterGeometry: WGeometry,
                      columnName: String): DataQuanta[Out] =
      spatialFilterJava(keySelector, spatialPredicate, filterGeometry, columnName)

    def spatialFilterJava(keySelector: SerializableFunction[Out, WGeometry],
                          spatialPredicate: SpatialPredicate,
                          filterGeometry: WGeometry,
                          columnName: String): DataQuanta[Out] = {
      implicit val planBuilder: PlanBuilder = self.planBuilder
      val spatialFilterOperator = new SpatialFilterOperator(
        spatialPredicate, keySelector, dataSetType[Out], filterGeometry
      )
      spatialFilterOperator.getKeyDescriptor.withSqlImplementation(null, columnName)
      self.connectTo(spatialFilterOperator, 0)
      wrap[Out](spatialFilterOperator)
    }

    /**
     * Feeds this and a further instance into a SpatialJoinOperator.
     *
     * @param thisKeyUdf UDF to extract keys from data quanta in this instance
     * @param that       the other instance
     * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
     * @param spatialPredicate the spatial predicate for the join
     * @return a new instance representing the SpatialJoinOperator's output
     */
    def spatialJoin[ThatOut: ClassTag](thisKeyUdf: Out => WGeometry,
                                       that: DataQuanta[ThatOut],
                                       thatKeyUdf: ThatOut => WGeometry,
                                       spatialPredicate: SpatialPredicate): DataQuanta[WayangTuple2[Out, ThatOut]] =
      spatialJoinJava(toSerializableFunction(thisKeyUdf), that, toSerializableFunction(thatKeyUdf), spatialPredicate)

    /**
     * Feeds this and a further instance into a SpatialJoinOperator.
     *
     * @param thisKeyUdf UDF to extract keys from data quanta in this instance
     * @param that       the other instance
     * @param thatKeyUdf UDF to extract keys from data quanta from `that` instance
     * @param spatialPredicate the spatial predicate for the join
     * @return a new instance representing the SpatialJoinOperator's output
     */
    def spatialJoinJava[ThatOut: ClassTag](thisKeyUdf: SerializableFunction[Out, WGeometry],
                                           that: DataQuanta[ThatOut],
                                           thatKeyUdf: SerializableFunction[ThatOut, WGeometry],
                                           spatialPredicate: SpatialPredicate): DataQuanta[WayangTuple2[Out, ThatOut]] = {
      require(self.planBuilder eq that.planBuilder, s"$self and $that must use the same plan builders.")
      implicit val planBuilder: PlanBuilder = self.planBuilder
      val spatialJoinOperator = new SpatialJoinOperator(
        new TransformationDescriptor(thisKeyUdf, basicDataUnitType[Out], basicDataUnitType[WGeometry]),
        new TransformationDescriptor(thatKeyUdf, basicDataUnitType[ThatOut], basicDataUnitType[WGeometry]),
        spatialPredicate
      )
      self.connectTo(spatialJoinOperator, 0)
      that.connectTo(spatialJoinOperator, 1)
      wrap[WayangTuple2[Out, ThatOut]](spatialJoinOperator)
    }
  }
}

/**
 * DataQuantaBuilder implementation for SpatialFilterOperator.
 *
 * Note: This class has two constructor forms:
 * - For Scala: Uses implicit JavaPlanBuilder parameter
 * - For Java: Pass JavaPlanBuilder as the 5th parameter explicitly
 */
class SpatialFilterDataQuantaBuilder[T](inputDataQuanta: DataQuantaBuilder[_, T],
                                        keySelector: SerializableFunction[T, WGeometry],
                                        spatialPredicate: SpatialPredicate,
                                        filterGeometry: WGeometry)
                                       (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[SpatialFilterDataQuantaBuilder[T], T] {

  import SpatialDataQuantaImplicits._

  private var columnName: String = _

  def withSqlGeometryColumnName(columnName: String): SpatialFilterDataQuantaBuilder[T] = {
    this.columnName = columnName
    this
  }

  override protected def build: DataQuanta[T] = {
    val dq = inputDataQuanta.dataQuanta()
    new SpatialDataQuanta(dq)(inputDataQuanta.classTag).spatialFilterJava(
      keySelector, spatialPredicate, filterGeometry, this.columnName
    )
  }
}

/**
 * DataQuantaBuilder implementation for SpatialJoinOperator.
 */
class SpatialJoinDataQuantaBuilder[In0, In1](inputDataQuanta0: DataQuantaBuilder[_, In0],
                                             inputDataQuanta1: DataQuantaBuilder[_, In1],
                                             keyUdf0: SerializableFunction[In0, WGeometry],
                                             keyUdf1: SerializableFunction[In1, WGeometry],
                                             spatialPredicate: SpatialPredicate)
                                            (implicit javaPlanBuilder: JavaPlanBuilder)
  extends BasicDataQuantaBuilder[SpatialJoinDataQuantaBuilder[In0, In1], WayangTuple2[In0, In1]] {

  import SpatialDataQuantaImplicits._

  override protected def build: DataQuanta[WayangTuple2[In0, In1]] = {
    val dq0 = inputDataQuanta0.dataQuanta()
    val dq1 = inputDataQuanta1.dataQuanta()
    applyTargetPlatforms(
      new SpatialDataQuanta(dq0)(inputDataQuanta0.classTag).spatialJoinJava(keyUdf0, dq1, keyUdf1, spatialPredicate)(inputDataQuanta1.classTag),
      this.getTargetPlatforms()
    )
  }
}
