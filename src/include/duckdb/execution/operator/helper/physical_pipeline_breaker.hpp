//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_pipeline_breaker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <iostream>

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class BreakerConcurrentQueue;
class ChunkBuffer;

//! PhysicalPipelineBreaker represents a physical operator that is used to break up pipelines
class PhysicalPipelineBreaker : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::PIPELINE_BREAKER;

public:
	PhysicalPipelineBreaker(vector<LogicalType> types, unique_ptr<PhysicalOperator> join, idx_t estimated_cardinality);

	~PhysicalPipelineBreaker() override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;

	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;

	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

public:
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;

	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	bool ParallelSource() const override {
		return true;
	}

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

private:
	unique_ptr<BreakerConcurrentQueue> chunk_queue;
};

}  // namespace duckdb