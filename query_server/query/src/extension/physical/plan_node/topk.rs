use std::{
    any::Any,
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{build_compare, make_array, ArrayData, ArrayRef, MutableArrayData},
        datatypes::SchemaRef,
        error::ArrowError,
        record_batch::RecordBatch,
    },
    error::DataFusionError,
    execution::{
        context::TaskContext, memory_manager::ConsumerType, runtime_env::RuntimeEnv,
        MemoryConsumer, MemoryConsumerId, MemoryManager,
    },
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        common::{batch_byte_size, SizedRecordBatchStream},
        metrics::{BaselineMetrics, CompositeMetricsSet, MemTrackingMetrics, MetricsSet},
        stream::RecordBatchStreamAdapter,
        DisplayFormatType, Distribution, EmptyRecordBatchStream, ExecutionPlan, Partitioning,
        SendableRecordBatchStream, Statistics,
    },
};

use datafusion::error::Result;
use futures::{StreamExt, TryFutureExt, TryStreamExt};
use trace::debug;

use crate::extension::logical::plan_node::topk::{TopKOptions, TopKStep};

use self::internal::{RecordBatchReference, Row};

const NOT_FOUND_MAX_ROW: &str = "max row not found.";

pub struct TopKExec {
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    options: TopKOptions,
    /// Containing all metrics set created during sort
    metrics_set: CompositeMetricsSet,
}

impl TopKExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        expr: Vec<PhysicalSortExpr>,
        options: TopKOptions,
    ) -> Self {
        Self {
            input,
            expr,
            options,
            metrics_set: CompositeMetricsSet::new(),
        }
    }
}

impl Debug for TopKExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TopKExec")
    }
}

#[async_trait]
impl ExecutionPlan for TopKExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn output_partitioning(&self) -> Partitioning {
        match self.options.step {
            TopKStep::SINGLE | TopKStep::FINAL => Partitioning::UnknownPartitioning(1),
            TopKStep::PARTIAL => self.input.output_partitioning(),
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        Some(&self.expr)
    }

    fn relies_on_input_order(&self) -> bool {
        false
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn required_child_distribution(&self) -> Distribution {
        // TODO Distributed topk is not supported yet
        match self.options.step {
            TopKStep::SINGLE | TopKStep::FINAL => Distribution::SinglePartition,
            TopKStep::PARTIAL => Distribution::UnspecifiedDistribution,
        }
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(TopKExec {
            input: children[0].clone(),
            expr: self.expr.clone(),
            options: self.options.clone(),
            metrics_set: self.metrics_set.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            "Start TopKExec::execute for partition {} of context session_id {} and task_id {:?}",
            partition,
            context.session_id(),
            context.task_id()
        );

        debug!(
            "Start invoking TopKExec's input.execute for partition: {}",
            partition
        );

        let input = self.input.execute(partition, context.clone())?;

        debug!("End TopKExec's input.execute for partition: {}", partition);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                do_top_k(
                    input,
                    partition,
                    self.expr.clone(),
                    self.metrics_set.clone(),
                    context,
                    self.options.clone(),
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
            )
            .try_flatten(),
        )))
    }

    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let expr: Vec<String> = self.expr.iter().map(|e| e.to_string()).collect();

                write!(f, "TopKExec: [{}], {}", expr.join(","), self.options,)
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics_set.aggregate_all())
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

async fn do_top_k(
    mut input: SendableRecordBatchStream,
    partition_id: usize,
    exprs: Vec<PhysicalSortExpr>,
    metrics_set: CompositeMetricsSet,
    context: Arc<TaskContext>,
    options: TopKOptions,
) -> Result<SendableRecordBatchStream> {
    debug!(
        "Start do_top_k({}) for partition {} of context session_id {} and task_id {:?}",
        &options,
        partition_id,
        context.session_id(),
        context.task_id()
    );
    let schema = input.schema();
    let tracking_metrics =
        metrics_set.new_intermediate_tracking(partition_id, context.runtime_env());
    let mut sorter = TopKBuilder::new(
        partition_id,
        schema.clone(),
        // exprs,
        metrics_set,
        // Arc::new(context.session_config()),
        context.runtime_env(),
        SimpleRecordBatchWithPositionComparator { exprs },
        options.clone(),
    );
    context.runtime_env().register_requester(sorter.id());
    while let Some(batch) = input.next().await {
        let batch = batch?;
        sorter
            .process_record_batch(batch, &tracking_metrics)
            .await?;
    }
    let result = sorter.build_result_by_row().await;
    debug!(
        "End do_top_k({}) for partition {} of context session_id {} and task_id {:?}",
        &options,
        partition_id,
        context.session_id(),
        context.task_id()
    );
    result
}

type PriorityQueue<T> = BinaryHeap<T>;

struct TopKBuilder<C> {
    id: MemoryConsumerId,
    schema: SchemaRef,
    /// Sort expressions
    runtime: Arc<RuntimeEnv>,
    metrics_set: CompositeMetricsSet,
    metrics: BaselineMetrics,
    // comparator: impl RecordBatchWithPositionComparator,
    comparator: Arc<C>,
    options: TopKOptions,

    // a map of heaps, each of which records the top N rows
    top_rows: PriorityQueue<Row<C>>,
    // a list of input record batches, each of which has information of which row in which heap references which position
    record_batch_references: HashMap<usize, RecordBatchReference>,

    current_batch_id: usize,
}

impl<C> TopKBuilder<C>
where
    C: RecordBatchWithPositionComparator,
    C: Send + Sync,
{
    pub fn new(
        partition_id: usize,
        schema: SchemaRef,
        metrics_set: CompositeMetricsSet,
        runtime: Arc<RuntimeEnv>,
        comparator: C,
        options: TopKOptions,
    ) -> Self {
        let metrics = metrics_set.new_intermediate_baseline(partition_id);

        Self {
            id: MemoryConsumerId::new(partition_id),
            schema,
            runtime,
            metrics_set,
            metrics,
            comparator: Arc::new(comparator),
            top_rows: PriorityQueue::with_capacity(options.k),
            options,
            record_batch_references: HashMap::default(),
            current_batch_id: 0,
        }
    }
}

impl<C> TopKBuilder<C>
where
    C: RecordBatchWithPositionComparator,
    C: std::fmt::Debug,
    C: Send + Sync,
{
    fn id(&self) -> &MemoryConsumerId {
        &self.id
    }

    async fn process_record_batch(
        &mut self,
        input: RecordBatch,
        _tracking_metrics: &MemTrackingMetrics,
    ) -> Result<()> {
        if input.num_rows() > 0 {
            // record metrics
            let size = batch_byte_size(&input);
            self.try_grow(size).await?;
            self.metrics.mem_used().add(size);

            // update the affected heaps and record candidate pages that need compaction
            let mut record_batch_to_compact: HashSet<usize> = HashSet::default();

            let mut new_record_batch_reference = self.wrap_record_batch_reference(&input);

            for row_num in 0..new_record_batch_reference.record_batch().num_rows() {
                if !self.topk_queue_is_full() {
                    // If the existing data is less than k, insert it directly
                    self.construct_row_and_save_it(&mut new_record_batch_reference, row_num);
                } else {
                    // may compare with the topN-th element with in the heap to decide if update is necessary
                    let nth_row = self.nth_row()?;

                    let nth_record_batch_id = nth_row.record_batch_id();

                    if self.comparator.compare(
                        &input,
                        row_num,
                        nth_row.record_batch(),
                        nth_row.row_num(),
                    )? == Ordering::Less
                    {
                        // The new data is smaller than the old data, remove the old data
                        // Delete the current record from the reference of the batch where the old data is located
                        self.remove_saved_max_row(&mut new_record_batch_reference)?;

                        // Construct a new record and add it to the reference
                        self.construct_row_and_save_it(&mut new_record_batch_reference, row_num);

                        // compact a record batch if it is not the current input page and the reference count is below the threshold
                        if self.need_compact(nth_record_batch_id)
                            && nth_record_batch_id != new_record_batch_reference.id()
                        {
                            record_batch_to_compact.insert(nth_record_batch_id);
                        }
                    }
                }
            }
            // This batch is referenced, it is added to the reference list
            self.reference_record_batch_if_necessary(new_record_batch_reference);

            // compact record batch references
            self.compact_record_batches_if_necessary(record_batch_to_compact)?;
        }

        Ok(())
    }

    async fn build_result_by_row(mut self) -> Result<SendableRecordBatchStream> {
        // Initialize a basket of generic variables
        let tracking_metrics = self
            .metrics_set
            .new_final_tracking(self.partition_id(), self.runtime.clone());
        let schema = self.schema.clone();
        let col_number = self.schema.fields().len();
        let record_batch_number = self.record_batch_references.len();

        // No result directly returns EmptyRecordBatchStream
        if record_batch_number == 0 {
            return Ok(Box::pin(EmptyRecordBatchStream::new(schema)));
        }

        // 1. Construct the mapping relationship of id -> record_batch's rank_number
        // 2. And according to the rank order, save the ArrayData of each column in each RecordBatch in the form of col_N -> [record_batch_1_N, record_batch_2_N, ...] to facilitate the subsequent construction of MutArrayData
        let mut merged_columns: Vec<Vec<&ArrayData>> =
            vec![Vec::with_capacity(record_batch_number); col_number];
        let id_to_rank_record_batch: HashMap<usize, usize> = self
            .record_batch_references
            .iter()
            .enumerate()
            .map(|(r_idx, (r_id, r))| {
                for (col_idx, col) in r.record_batch().columns().iter().enumerate() {
                    if let Some(r_array_datas) = merged_columns.get_mut(col_idx) {
                        let data = col.data();
                        r_array_datas.push(data);
                    }
                }

                (r_id.to_owned(), r_idx)
            })
            .collect();

        // 3. Construct MutableArrayData for each column
        let mut mut_array_datas: Vec<MutableArrayData> = merged_columns
            .iter()
            .map(|col| MutableArrayData::new(col.to_owned(), true, self.options.k))
            .collect();

        // 4. Get the positive index of the topK record
        // construct sorting index
        let mut sorted_topk_row: Vec<Row<C>> = Vec::with_capacity(self.options.k);
        // Take the sorted records out of the priority queue, in descending order
        while let Some(row) = self.top_rows.pop() {
            sorted_topk_row.push(row);
        }
        // reverse to positive sequence
        sorted_topk_row.reverse();

        // 5. According to the index order, fill the value of MutableArrayData of each column
        // loop through all rows
        for row in sorted_topk_row.iter() {
            if let Some(rank) = id_to_rank_record_batch.get(&row.record_batch_id()) {
                // 遍历行的所有列
                for col in mut_array_datas.iter_mut() {
                    col.extend(*rank, row.row_num(), row.row_num() + 1);
                }
            }
        }

        // 6. Construct the result
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(col_number);

        for ele in mut_array_datas {
            let array_ref = make_array(ele.freeze());
            columns.push(array_ref);
        }

        let columns = vec![Arc::new(RecordBatch::try_new(schema.clone(), columns)?)];

        let result = SizedRecordBatchStream::new(schema, columns, tracking_metrics);

        // release occupied memory
        self.free_all_memory();

        Ok(Box::pin(result))
    }

    fn nth_row(&self) -> Result<&Row<C>> {
        let nth_row = self
            .top_rows
            .peek()
            .ok_or_else(|| DataFusionError::Execution(NOT_FOUND_MAX_ROW.to_string()))?;

        Ok(nth_row)
    }

    fn compact_record_batches_if_necessary(
        &mut self,
        record_batch_to_compact: HashSet<usize>,
    ) -> Result<()> {
        for id in record_batch_to_compact {
            if let Some(mut reference) = self.record_batch_references.remove(&id) {
                if reference.used_row_count() == 0 {
                    // Directly delete, release resources
                    self.record_batch_references.remove(&id);
                } else {
                    // perform compact, release resources
                    reference.compaction_v2()?;
                }
            }
        }

        Ok(())
    }

    fn need_compact(&self, nth_record_batch_id: usize) -> bool {
        // support compact
        self.record_batch_references
            .get(&nth_record_batch_id)
            .map_or_else(
                || false,
                |e| {
                    // More than half of the invalid records need to be compacted, otherwise memory is wasted
                    e.record_batch().num_rows() > e.used_row_count() * 2
                },
            );
        false
    }

    fn reference_record_batch_if_necessary(&mut self, reference: RecordBatchReference) {
        if reference.used_row_count() > 0 {
            self.record_batch_references
                .insert(reference.id(), reference);
        }
    }

    fn wrap_record_batch_reference(&mut self, record_batch: &RecordBatch) -> RecordBatchReference {
        self.current_batch_id += 1;

        RecordBatchReference::new(self.current_batch_id, record_batch)
    }

    fn topk_queue_is_full(&self) -> bool {
        self.top_rows.len() >= self.options.k
    }

    fn remove_saved_max_row(
        &mut self,
        default_record_batch_reference: &mut RecordBatchReference,
    ) -> Result<()> {
        let max_row = self
            .top_rows
            .pop()
            .ok_or_else(|| DataFusionError::Execution(NOT_FOUND_MAX_ROW.to_string()))?;

        // remove the reference in record_batch_reference
        self.record_batch_references
            .get_mut(&max_row.record_batch_id())
            // The id is not in the references to indicate that it is the current RecordBatch
            .unwrap_or(default_record_batch_reference)
            .dereference(max_row.row_num());

        Ok(())
    }

    fn construct_row_and_save_it(
        &mut self,
        record_batch_reference: &mut RecordBatchReference,
        row_num: usize,
    ) {
        // construct row
        let new_row: Row<C> = Row::new(
            self.comparator.clone(),
            record_batch_reference.record_batch().clone(),
            record_batch_reference.id(),
            row_num,
        );
        // Associate the new row with record_batch_reference
        record_batch_reference.reference(new_row.row_num());
        // save the new row to the topn queue
        self.top_rows.push(new_row);
    }

    fn free_all_memory(&self) -> usize {
        let used = self.metrics.mem_used().set(0);
        self.shrink(used);
        used
    }
}

#[async_trait]
impl<C> MemoryConsumer for TopKBuilder<C>
where
    C: Send + Sync,
{
    fn name(&self) -> String {
        "TopKBuilder".to_owned()
    }

    fn id(&self) -> &MemoryConsumerId {
        &self.id
    }

    fn memory_manager(&self) -> Arc<MemoryManager> {
        self.runtime.memory_manager.clone()
    }

    fn type_(&self) -> &ConsumerType {
        &ConsumerType::Requesting
    }

    async fn spill(&self) -> Result<usize> {
        let used = self.metrics.mem_used().value();
        Ok(used)
    }

    fn mem_used(&self) -> usize {
        self.metrics.mem_used().value()
    }
}

trait RecordBatchWithPositionComparator {
    fn compare(
        &self,
        left: &RecordBatch,
        left_position: usize,
        right: &RecordBatch,
        right_position: usize,
    ) -> Result<Ordering>;
}

#[derive(Debug)]
struct SimpleRecordBatchWithPositionComparator {
    exprs: Vec<PhysicalSortExpr>,
}

impl RecordBatchWithPositionComparator for SimpleRecordBatchWithPositionComparator {
    fn compare(
        &self,
        left: &RecordBatch,
        left_position: usize,
        right: &RecordBatch,
        right_position: usize,
    ) -> Result<Ordering> {
        for sort_expr in self.exprs.iter() {
            let order_opts = sort_expr.options;

            let left_block = sort_expr.expr.evaluate(left)?;
            let right_block = sort_expr.expr.evaluate(right)?;

            let left_values = left_block.into_array(left.num_rows());
            let right_values = right_block.into_array(right.num_rows());

            let comparator = build_compare(left_values.as_ref(), right_values.as_ref())?;

            match (comparator)(left_position, right_position) {
                // equal, move on to next column
                Ordering::Equal => continue,
                order => {
                    if order_opts.descending {
                        return Ok(order.reverse());
                    } else {
                        return Ok(order);
                    }
                }
            }
        }

        debug!("{} Equal {}", left_position, right_position);
        Ok(Ordering::Equal)
    }
}

mod internal {
    use datafusion::error::Result;
    use std::{cmp::Ordering, sync::Arc};
    use trace::error;

    use datafusion::arrow::{
        array::BooleanArray, compute::filter_record_batch, record_batch::RecordBatch,
    };

    use super::RecordBatchWithPositionComparator;

    #[derive(Debug, Clone)]
    pub struct Row<C> {
        comparator: Arc<C>,
        record_batch: RecordBatch,
        record_batch_id: usize,
        row_num: usize,
    }

    impl<C> Row<C> {
        #[inline(always)]
        pub fn new(
            comparator: Arc<C>,
            record_batch: RecordBatch,
            record_batch_id: usize,
            row_num: usize,
        ) -> Self {
            // let id = ((record_batch_id as u64) << 32_u8) | ((row_num & 0xffffffff) as u64);

            Self {
                comparator,
                record_batch,
                record_batch_id,
                row_num,
            }
        }
    }

    impl<C> Row<C> {
        pub fn row_num(&self) -> usize {
            self.row_num
        }

        pub fn record_batch_id(&self) -> usize {
            self.record_batch_id
        }

        pub fn record_batch(&self) -> &RecordBatch {
            &self.record_batch
        }
    }

    impl<C> PartialEq for Row<C> {
        fn eq(&self, other: &Self) -> bool {
            self.record_batch == other.record_batch
                && self.record_batch_id == other.record_batch_id
                && self.row_num == other.row_num
        }
    }

    impl<C> Eq for Row<C> {}

    impl<C> PartialOrd for Row<C>
    where
        C: RecordBatchWithPositionComparator,
    {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.comparator
                .compare(
                    self.record_batch(),
                    self.row_num,
                    other.record_batch(),
                    other.row_num,
                )
                .ok()
        }
    }

    impl<C> Ord for Row<C>
    where
        C: RecordBatchWithPositionComparator,
    {
        fn cmp(&self, other: &Self) -> Ordering {
            self.partial_cmp(other).unwrap_or_else(|| {
                error!(
                    "compare record error, type mismatched. left: {}, right: {}",
                    self.record_batch().schema(),
                    other.record_batch().schema()
                );
                Ordering::Equal
            })
        }
    }

    #[derive(Clone)]
    pub struct RecordBatchReference {
        id: usize,
        record_batch: RecordBatch,
        // mask for used row
        reference: Vec<bool>,
        used_row_count: usize,
    }

    impl RecordBatchReference {
        pub fn new(id: usize, record_batch: &RecordBatch) -> Self {
            Self {
                id,
                reference: vec![false; record_batch.num_rows()],
                record_batch: record_batch.clone(),
                used_row_count: 0,
            }
        }

        pub fn id(&self) -> usize {
            self.id
        }

        pub fn record_batch(&self) -> &RecordBatch {
            &self.record_batch
        }

        pub fn used_row_count(&self) -> usize {
            self.used_row_count
        }

        /// Note that the row_num of new_row should not exceed the capacity of reference, otherwise panic
        /// No verification is done here
        pub fn reference(&mut self, row_num: usize) {
            self.reference[row_num] = true;
            self.used_row_count += 1;
        }

        /// Note that row_num should not exceed the capacity of reference, otherwise panic
        /// No verification is done here
        pub fn dereference(&mut self, row_num: usize) {
            self.reference[row_num] = false;
            self.used_row_count -= 1;
        }

        pub fn compaction_v2(&mut self) -> Result<()> {
            // If all records are valid, return directly
            if self.used_row_count() == self.record_batch().num_rows() {
                return Ok(());
            }

            // compact record batch
            self.record_batch = filter_record_batch(
                &self.record_batch,
                // mask for used row
                &BooleanArray::from(self.reference.clone()),
            )?;
            // re-assign reference
            self.reference = vec![true; self.used_row_count];

            Ok(())
        }
    }
}
