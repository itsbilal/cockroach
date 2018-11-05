// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type zigzagJoinNode struct {
	// sides contains information about each individual "side" of a
	// zigzag join. Must contain 2 or more zigzagJoinSides.
	sides []zigzagJoinSide

	// joinType is either INNER or LEFT_OUTER.
	joinType sqlbase.JoinType

	// columns are the produced columns, namely the columns in all
	// indexes in 'sides' - in the same order as sides.
	columns sqlbase.ResultColumns

	// onCond is any ON condition to be used in conjunction with the implicit
	// equality condition on keyCols.
	onCond tree.TypedExpr

	// fixedVals contains fixed values for a prefix of each side's index columns.
	fixedVals []*valuesNode

	props physicalProps

	// run is a joinNode used for any local runs of this planNode.
	run *joinNode
}

// zigzagJoinSide contains information about one "side" of the zigzag
// join. Note that the length of all eqCols in one zigzagJoinNode should
// be the same.
type zigzagJoinSide struct {
	// index for this side.
	index *scanNode

	// eqCols is an int slice containing the equated columns for this side
	// of the zigzag join.
	eqCols []int
}

// startExec is part of the execStartable interface.
func (zj *zigzagJoinNode) startExec(params runParams) error {
	// Make sure the table node has a span (full scan).
	leftSide := zj.sides[0]
	rightSide := zj.sides[1]

	// Create a joinNode that joins the input and the table. Note that startExec
	// will be called on zj.input and zj.table.

	leftSrc := planDataSource{
		info: &sqlbase.DataSourceInfo{SourceColumns: planColumns(leftSide.index)},
		plan: leftSide.index,
	}

	rightSrc := planDataSource{
		info: &sqlbase.DataSourceInfo{SourceColumns: planColumns(rightSide.index)},
		plan: rightSide.index,
	}

	// The lookup side may not output all the index columns on which we are doing
	// the lookup. We need them to be produced so that we can refer to them in the
	// join predicate. So we find any such instances and adjust the scan node
	// accordingly.
	for _, side := range zj.sides {
		for _, col := range side.eqCols {
			colID := side.index.index.ColumnIDs[col]
			if _, ok := side.index.colIdxMap[colID]; !ok {
				panic("lodestar")
			}
		}
	}
	/*
		for i := range zj.keyCols {
			colID := zj.table.index.ColumnIDs[i]
			if _, ok := zj.table.colIdxMap[colID]; !ok {
				// Tricky case: the lookup join doesn't output this column so we can't
				// refer to it; we have to add it.
				n := zj.table
				colPos := len(n.cols)
				var colDesc *sqlbase.ColumnDescriptor
				for i := range n.desc.Columns {
					if n.desc.Columns[i].ID == colID {
						colDesc = &n.desc.Columns[i]
						break
					}
				}
				n.cols = append(n.cols, *colDesc)
				n.resultColumns = append(
					n.resultColumns,
					leftSrc.info.SourceColumns[zj.keyCols[i]],
				)
				n.colIdxMap[colID] = colPos
				n.valNeededForCol.Add(colPos)
				n.run.row = make([]tree.Datum, len(n.cols))
				n.filterVars = tree.MakeIndexedVarHelper(n, len(n.cols))
				// startExec was already called for the node, run it again.
				if err := n.startExec(params); err != nil {
					return err
				}
			}
		}
	*/

	pred, _, err := params.p.makeJoinPredicate(
		context.TODO(), leftSrc.info, rightSrc.info, zj.joinType, nil, /* cond */
	)
	if err != nil {
		return err
	}

	// Program the equalities implied by eqCols.
	for i := range leftSide.eqCols {
		pred.addEquality(leftSrc.info, leftSide.eqCols[i], rightSrc.info, rightSide.eqCols[i])
	}

	onAndExprs := splitAndExpr(params.EvalContext(), zj.onCond, nil /* exprs */)
	for _, e := range onAndExprs {
		if e != tree.DBoolTrue && !pred.tryAddEqualityFilter(e, leftSrc.info, rightSrc.info) {
			pred.onCond = mergeConj(pred.onCond, e)
		}
	}
	zj.run = params.p.makeJoinNode(leftSrc, rightSrc, pred)
	return zj.run.startExec(params)
}

func (zj *zigzagJoinNode) Next(params runParams) (bool, error) {
	return zj.run.Next(params)
}

func (zj *zigzagJoinNode) Values() tree.Datums {
	// Chop off any values we may have tacked onto the table scanNode.
	return zj.run.Values()[:len(zj.columns)]
}

func (zj *zigzagJoinNode) Close(ctx context.Context) {
	if zj.run != nil {
		zj.run.Close(ctx)
	} else {
		for i := range zj.sides {
			zj.sides[i].index.Close(ctx)
		}
	}
}
