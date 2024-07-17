/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <algorithm>
#include <ranges>
#include <cstdint>
#include "sql/expr/aggregate_hash_table.h"
#include "sql/operator/physical_operator.h"

using namespace std;

/**
 * @brief Group By 物理算子(vectorized)
 * @ingroup PhysicalOperator
 */
class GroupByVecPhysicalOperator : public PhysicalOperator
{
public:
  GroupByVecPhysicalOperator(
      std::vector<std::unique_ptr<Expression>> &&group_by_exprs, std::vector<Expression *> &&expressions) {
        group_by_expressions_ = std::move(group_by_exprs);
        aggregate_expressions_ = std::move(expressions);
        value_expressions_.reserve(aggregate_expressions_.size());

        // hash_table_ = make_unique<StandardAggregateHashTable>(aggregate_expressions_);
        // scanner = StandardAggregateHashTable::Scanner(hash_table_.get());

        ranges::for_each(aggregate_expressions_, [this](Expression *expr) {
          auto *      aggregate_expr = static_cast<AggregateExpr *>(expr);
          Expression *child_expr     = aggregate_expr->child().get();
          ASSERT(child_expr != nullptr, "aggregation expression must have a child expression");
          value_expressions_.emplace_back(child_expr);
        });

        for (size_t group_idx = 0; group_idx < group_by_expressions_.size(); group_idx++) {
          auto &expr = group_by_expressions_[group_idx];
          output_chunk_.add_column(std::make_unique<Column>(expr->value_type(), expr->value_length()), group_idx);
        }

        for (size_t aggr_idx = 0; aggr_idx < aggregate_expressions_.size(); aggr_idx++) {
          auto &expr = value_expressions_[aggr_idx];
          output_chunk_.add_column(std::make_unique<Column>(expr->value_type(), expr->value_length()), group_by_expressions_.size() + aggr_idx);
        }

        
      }

  virtual ~GroupByVecPhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::GROUP_BY_VEC; }

  RC open(Trx *trx) override { 
    ASSERT(children_.size() == 1, "group by operator only support one child, but got %d", children_.size());

    PhysicalOperator &child = *children_[0];
    RC                rc    = child.open(trx);
    if (OB_FAIL(rc)) {
      LOG_INFO("failed to open child operator. rc=%s", strrc(rc));
      return rc;
    }

    group_chunk_.reset();
    aggr_chunk_.reset();

    // 处理成 group_chunk和aggr_chunk的形式
    // group_chunk |c1|c2|
    // aggr_chunk |a1|a2|
    // SELECT id+id, sum(num) + 1 + sum(num), price FROM aggregation_func group by id+id, price;
    while (OB_SUCC(rc = child.next(chunk_))) {
      // 接着我们需要获取Column
      for (size_t group_idx = 0; group_idx < group_by_expressions_.size(); group_idx++) {
        auto column = make_unique<Column>();
        group_by_expressions_[group_idx]->get_column(chunk_, *column);
        group_chunk_.add_column(std::move(column), group_idx);
      }

      for (size_t aggr_idx = 0; aggr_idx < aggregate_expressions_.size(); aggr_idx++) {
        auto column = make_unique<Column>();
        value_expressions_[aggr_idx]->get_column(chunk_, *column);
        aggr_chunk_.add_column(std::move(column), aggr_idx);
      }
    }

    if (rc == RC::RECORD_EOF) {
      rc = RC::SUCCESS;
    }

    return rc;
  }

  RC next(Chunk &chunk) override { 
    if (chunk_.capacity() == 0) {
      return RC::RECORD_EOF;
    }
    RC rc = RC::SUCCESS;

    output_chunk_.reset_data();

    auto hash_table = std::make_unique<StandardAggregateHashTable>(aggregate_expressions_);
    hash_table->add_chunk(group_chunk_, aggr_chunk_);

    StandardAggregateHashTable::Scanner scanner(hash_table.get());
    scanner.open_scan();
    rc = scanner.next(output_chunk_);

    rc = chunk.reference(output_chunk_);
    chunk_.reset();
    return rc;
  }

  RC close() override { 
    children_[0]->close();
    return RC::SUCCESS;
  }

private:
  std::vector<std::unique_ptr<Expression>>      group_by_expressions_;
  std::vector<Expression *>                     aggregate_expressions_;
  std::vector<Expression *>                     value_expressions_;
  Chunk                                         group_chunk_;
  Chunk                                         aggr_chunk_;
  Chunk                                         chunk_;
  Chunk                                         output_chunk_;
};