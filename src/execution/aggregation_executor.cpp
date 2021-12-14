//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan_->GetAggregates(), plan_->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  Tuple tuple;
  RID rid;
  try {
    // 将子查询结果插入到哈希表中
    while (child_->Next(&tuple, &rid)) {
      aht_.InsertCombine(MakeAggregateKey(&tuple), MakeAggregateValue(&tuple));
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "AggregationExecutor:child execute error.");
  }
  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  const AggregateKey &agg_key = aht_iterator_.Key();
  const AggregateValue &agg_value = aht_iterator_.Val();
  ++aht_iterator_;

  // 判断Having条件，符合返回，不符合则继续查找
  if (plan_->GetHaving() == nullptr ||
      plan_->GetHaving()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_).GetAs<bool>()) {
    std::vector<Value> ret;
    for (const auto &col : plan_->OutputSchema()->GetColumns()) {
      ret.push_back(col.GetExpr()->EvaluateAggregate(agg_key.group_bys_, agg_value.aggregates_));
    }
    *tuple = Tuple(ret, plan_->OutputSchema());
    return true;
  }
  return Next(tuple, rid);
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
