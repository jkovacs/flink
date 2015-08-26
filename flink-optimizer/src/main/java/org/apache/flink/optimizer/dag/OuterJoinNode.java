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

package org.apache.flink.optimizer.dag;

import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual;
import org.apache.flink.optimizer.operators.SortMergeFullOuterJoinDescriptor;
import org.apache.flink.optimizer.operators.SortMergeLeftOuterJoinDescriptor;
import org.apache.flink.optimizer.operators.SortMergeRightOuterJoinDescriptor;

import java.util.Collections;
import java.util.List;

public class OuterJoinNode extends TwoInputNode {

	private List<OperatorDescriptorDual> dataProperties;

	/**
	 * Creates a new two input node for the optimizer plan, representing the given operator.
	 *
	 * @param operator The operator that the optimizer DAG node should represent.
	 */
	public OuterJoinNode(JoinOperatorBase<?, ?, ?, ?> operator) {
		super(operator);

		this.dataProperties = getDataProperties();
	}

	private List<OperatorDescriptorDual> getDataProperties() {
		JoinOperatorBase.JoinType type = getOperator().getJoinType();
		// TODO: clean up & respect internal or user hints when selecting join descriptors
		if (type == JoinOperatorBase.JoinType.FULL_OUTER) {
			return Collections.<OperatorDescriptorDual>singletonList(new SortMergeFullOuterJoinDescriptor(this.keys1, this.keys2));
		} else if (type == JoinOperatorBase.JoinType.LEFT_OUTER) {
			return Collections.<OperatorDescriptorDual>singletonList(new SortMergeLeftOuterJoinDescriptor(this.keys1, this.keys2));
		} else if (type == JoinOperatorBase.JoinType.RIGHT_OUTER) {
			return Collections.<OperatorDescriptorDual>singletonList(new SortMergeRightOuterJoinDescriptor(this.keys1, this.keys2));
		} else {
			throw new RuntimeException();
		}
	}

	@Override
	public JoinOperatorBase<?, ?, ?, ?> getOperator() {
		return (JoinOperatorBase<?, ?, ?, ?>) super.getOperator();
	}

	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		return dataProperties;
	}

	@Override
	public String getName() {
		return "Outer Join";
	}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		JoinOperatorBase.JoinType type = getOperator().getJoinType();

		long card1 = getFirstPredecessorNode().getEstimatedNumRecords();
		long card2 = getSecondPredecessorNode().getEstimatedNumRecords();

		if (card1 < 0 || card2 < 0) {
			this.estimatedNumRecords = -1;
		} else {
			if (type == JoinOperatorBase.JoinType.LEFT_OUTER) {
				this.estimatedNumRecords = card1;
			} else if (type == JoinOperatorBase.JoinType.RIGHT_OUTER) {
				this.estimatedNumRecords = card2;
			} else if (type == JoinOperatorBase.JoinType.FULL_OUTER) {
				this.estimatedNumRecords = Math.max(card1, card2);
			}
		}

		if (this.estimatedNumRecords >= 0) {
			float width1 = getFirstPredecessorNode().getEstimatedAvgWidthPerOutputRecord();
			float width2 = getSecondPredecessorNode().getEstimatedAvgWidthPerOutputRecord();
			float width = (width1 <= 0 || width2 <= 0) ? -1 : width1 + width2;

			if (width > 0) {
				this.estimatedOutputSize = (long) (width * this.estimatedNumRecords);
			}
		}
	}
}
