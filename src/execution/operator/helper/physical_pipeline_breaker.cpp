#include "duckdb/execution/operator/helper/physical_pipeline_breaker.hpp"

#include "../extension/jemalloc/include/jemalloc_extension.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/column/column_data_collection_segment.hpp"

extern int numa_tag;

namespace duckdb {

class ChunkBuffer {
public:
	ChunkBuffer(Allocator &allocator, const vector<LogicalType> &types) : collection(allocator, types) {
		collection.InitializeAppend(append_state);
		segment = collection.GetSegments()[0].get();
		column_ids.reserve(types.size());
		for (idx_t i = 0; i < types.size(); i++) {
			column_ids.push_back(i);
		}
	}

	void Append(DataChunk &chunk) {
		collection.Append(append_state, chunk);
	}

	void Scan(ChunkMetaData &chunk_meta, DataChunk &chunk, ChunkManagementState &lstate) {
		chunk.Reset();
		lstate.handles.clear();
		segment->ReadChunkFromChunkMeta(chunk_meta, lstate, chunk, column_ids);
	}

	ChunkMetaData& FetchChunkMeta(idx_t chunk_index) {
		return segment->chunk_data[chunk_index];
	}

	idx_t ChunkCount() {
		return segment->ChunkCount();
	}

private:
	ColumnDataCollection collection;
	ColumnDataAppendState append_state;
	ColumnDataCollectionSegment *segment;
	vector<column_t> column_ids;
};

void ConcurrentQueue::Enqueue(ChunkReference &&chunk_ref) {
	if (q.enqueue(std::move(chunk_ref))) {
		semaphore.signal();
	} else {
		throw InternalException("Could not enqueue datachunk!");
	}
}

bool ConcurrentQueue::TryDequeue(ChunkReference &chunk_ref) {
	semaphore.wait();
	return q.try_dequeue(chunk_ref);
}

void ConcurrentQueue::Finalize() {
	semaphore.signal(96);
}

PhysicalPipelineBreaker::PhysicalPipelineBreaker(vector<LogicalType> types, unique_ptr<PhysicalOperator> child_operator,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::PIPELINE_BREAKER, std::move(types), estimated_cardinality),
	  chunk_queue(make_uniq<ConcurrentQueue>()) {
	children.push_back(std::move(child_operator));
}

PhysicalPipelineBreaker::~PhysicalPipelineBreaker() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class PipelineBreakerSinkState : public LocalSinkState {
public:
	PipelineBreakerSinkState(Allocator &allocator, const vector<LogicalType> &types)
	: buffer(make_shared_ptr<ChunkBuffer>(allocator, types)), added_chunk(0) {}
public:
	shared_ptr<ChunkBuffer> buffer;
	idx_t added_chunk;
};

unique_ptr<LocalSinkState> PhysicalPipelineBreaker::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<PipelineBreakerSinkState>(Allocator::DefaultAllocator(), types);
}

SinkResultType PhysicalPipelineBreaker::Sink(ExecutionContext &context, DataChunk &chunk,
											 OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerSinkState>();
	lstate.buffer->Append(chunk);
	while (lstate.added_chunk + 1 < lstate.buffer->ChunkCount()) {
		ChunkReference chunk_ref{lstate.buffer, std::move(lstate.buffer->FetchChunkMeta(lstate.added_chunk))};
		chunk_queue->Enqueue(std::move(chunk_ref));
		lstate.added_chunk++;
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalPipelineBreaker::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerSinkState>();
	while (lstate.added_chunk < lstate.buffer->ChunkCount()) {
		ChunkReference chunk_ref{lstate.buffer, std::move(lstate.buffer->FetchChunkMeta(lstate.added_chunk))};
		chunk_queue->Enqueue(std::move(chunk_ref));
		lstate.added_chunk++;
	}
	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalPipelineBreaker::Finalize(Pipeline &pipeline, Event &event,
                                                   ClientContext &context,
                                                   OperatorSinkFinalizeInput &input) const {
	chunk_queue->Finalize();
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PipelineBreakerGlobalSource : public GlobalSourceState {
	idx_t MaxThreads() override {
		return 16;
	}
};

unique_ptr<GlobalSourceState> PhysicalPipelineBreaker::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<PipelineBreakerGlobalSource>();
}

class PipelineBreakerLocalSource : public LocalSourceState {
public:
	PipelineBreakerLocalSource() {
		scan_state.properties = ColumnDataScanProperties::ALLOW_ZERO_COPY;
	}
public:
	ChunkManagementState scan_state;
};

unique_ptr<LocalSourceState> PhysicalPipelineBreaker::GetLocalSourceState(ExecutionContext &context,
																		  GlobalSourceState &gstate) const {
	return make_uniq<PipelineBreakerLocalSource>();
}

SourceResultType PhysicalPipelineBreaker::GetData(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &lstate = input.local_state.Cast<PipelineBreakerLocalSource>();
	ChunkReference chunk_ref;
	if (chunk_queue->TryDequeue(chunk_ref)) {
		chunk_ref.buffer->Scan(chunk_ref.chunk_meta, chunk, lstate.scan_state);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	return SourceResultType::FINISHED;
}

// Build
void PhysicalPipelineBreaker::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();

	// operator is a sink, build a pipeline
	sink_state.reset();
	D_ASSERT(children.size() == 1);

	// single operator: the operator becomes the data source of the current pipeline
	state.SetPipelineSource(current, *this);

	// NUMATODO : add config
	bool allow_concurrent = true;
	// we create a new pipeline starting from the child
	if (allow_concurrent) {
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipelineWithoutDependency(current, *this);
		child_meta_pipeline.Build(*children[0]);
		for (auto &dependence_p : child_meta_pipeline.GetBasePipeline()->GetDependencies()) {
			auto dependence = dependence_p.lock();
			current.AddDependency(dependence);
		}
	} else {
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		child_meta_pipeline.Build(*children[0]);
	}
}

}  // namespace duckdb