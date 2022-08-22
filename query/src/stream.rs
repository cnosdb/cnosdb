use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::{thread, time};

use datafusion::arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::{
    arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch},
    physical_plan::RecordBatchStream,
};
use futures::Stream;
use parking_lot::Mutex;

use models::SeriesId;
use trace::{debug, error};
use tskv::engine::EngineRef;
use tskv::memcache::DataType as MDataType;
use tskv::tsm::DataBlock;
use tskv::TimeRange;

use crate::predicate::PredicateRef;
use crate::schema::{FIELD_ID, TAG};

pub const TIME_FIELD: &str = "time";

enum ArrayType {
    U64(Vec<u64>),
    I64(Vec<i64>),
    Str(Vec<String>),
    F64(Vec<f64>),
    Bool(Vec<bool>),
}

pub struct TableScanStream {
    db_name: String,
    table_name: String,
    data: Arc<Mutex<Vec<RecordBatch>>>,
    index: usize,
    proj_schema: SchemaRef,
    filter: PredicateRef,
    batch_size: usize,
    store_engine: EngineRef,
    status: bool,
}

impl TableScanStream {
    pub fn new(
        db_name: String,
        table_name: String,
        proj_schema: SchemaRef,
        filter: PredicateRef,
        batch_size: usize,
        store_engine: EngineRef,
    ) -> Self {
        Self {
            db_name,
            table_name,
            data: Arc::new(Mutex::new(vec![])),
            index: 0,
            proj_schema,
            filter,
            batch_size,
            store_engine,
            status: false,
        }
    }
}

type ArrowResult<T> = Result<T, ArrowError>;

impl Stream for TableScanStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if !this.status {
            this.status = true;
            let store_engine = this.store_engine.clone();
            let db_name = this.db_name.clone();
            let table_name = this.table_name.clone();
            let proj_schema = this.proj_schema.clone();
            let data = this.data.clone();
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?;
            rt.block_on(async move {
                //todo: for test, it will remove after predicate done
                let sids = store_engine
                    .get_series_id_list(&db_name, &table_name, &vec![])
                    .await
                    .unwrap();

                let fields = get_field_ids(proj_schema.clone());
                debug!("sids {:?}, fields {:?}", sids, fields);
                let block_map = store_engine.read(
                    &db_name,
                    sids,
                    &TimeRange {
                        min_ts: i64::MIN,
                        max_ts: i64::MAX,
                    },
                    fields,
                );
                debug!("printf block map {:?}", block_map);
                let mut batch = make_record_batch(
                    &db_name,
                    block_map,
                    store_engine.clone(),
                    proj_schema.clone(),
                );
                data.lock().append(&mut batch);
            });
        };

        return if this.data.lock().is_empty() {
            thread::sleep(time::Duration::from_millis(1000));
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        } else if this.data.lock().len() <= this.index {
            std::task::Poll::Ready(None)
        } else {
            let ans = this.data.lock()[this.index].clone();
            this.index += 1;
            debug!("input record batch index:{:?}, ans {:?}", this.index, ans);
            std::task::Poll::Ready(Some(Ok(ans)))
        };
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // todo   (self.data.len(), Some(self.data.len()))
        (0, Some(0))
    }
}

impl RecordBatchStream for TableScanStream {
    fn schema(&self) -> SchemaRef {
        self.proj_schema.clone()
    }
}

fn get_field_ids(proj_schema: SchemaRef) -> Vec<u32> {
    let mut fields = vec![];
    for i in proj_schema.fields() {
        if let Some(meta_data) = i.metadata() {
            if let Some(is_tag) = meta_data.get(TAG) {
                let tag: bool = FromStr::from_str(is_tag).unwrap();
                if tag {
                    continue;
                }
            }
            if let Some(field_id) = meta_data.get(FIELD_ID) {
                fields.push(u32::from_str(field_id).unwrap());
            }
        }
    }
    fields
}

#[allow(clippy::comparison_chain)]
fn push_record_array(
    field_id: u32,
    entry: &mut ArrayType,
    field_array_index: &mut HashMap<u32, i32>,
    data_blocks: &Vec<DataBlock>,
    ts_array: &Vec<i64>,
    ts_array_index: usize,
) {
    let index = field_array_index.entry(field_id).or_insert(0);
    let vec_index = (*index / 1000) as usize;
    let mut block_index = (*index % 1000) as usize;
    if data_blocks.len() <= vec_index {
        return;
    }
    if data_blocks[vec_index].len() <= block_index {
        return;
    }
    debug!(
        "val ts:{:?}, ts_array: {:?}",
        data_blocks[vec_index].ts(),
        ts_array
    );
    debug!("block:{:?}, ts_array:{:?}", block_index, ts_array_index);
    let len = data_blocks[vec_index].ts().len();
    while block_index < len {
        if data_blocks[vec_index].ts()[block_index] > ts_array[ts_array_index] {
            match entry {
                ArrayType::U64(v) => {
                    v.push(0);
                }
                ArrayType::I64(v) => {
                    v.push(0);
                }
                ArrayType::F64(v) => {
                    v.push(0 as f64);
                }
                ArrayType::Str(v) => {
                    v.push("".to_string());
                }
                ArrayType::Bool(v) => {
                    v.push(false);
                }
            }
            break;
        } else if data_blocks[vec_index].ts()[block_index] == ts_array[ts_array_index] {
            match entry {
                ArrayType::U64(v) => {
                    if let MDataType::U64(cell) = data_blocks[vec_index].get(block_index).unwrap() {
                        v.push(cell.val);
                    }
                }
                ArrayType::I64(v) => {
                    if let MDataType::I64(cell) = data_blocks[vec_index].get(block_index).unwrap() {
                        v.push(cell.val);
                    }
                }
                ArrayType::F64(v) => {
                    if let MDataType::F64(cell) = data_blocks[vec_index].get(block_index).unwrap() {
                        v.push(cell.val);
                    }
                }
                ArrayType::Str(v) => {
                    if let MDataType::Str(cell) = data_blocks[vec_index].get(block_index).unwrap() {
                        v.push(String::from_utf8(cell.val).unwrap());
                    }
                }
                ArrayType::Bool(v) => {
                    if let MDataType::Bool(cell) = data_blocks[vec_index].get(block_index).unwrap()
                    {
                        v.push(cell.val);
                    }
                }
            }
            *index += 1;
            break;
        }
        block_index += 1;
    }
    //  else {
    //     // *index += 1;
    //     //panic!("ts order error!");
    // }
}

fn push_record_batch(
    fid: u32,
    array_type: ArrayType,
    batch_array_vec: &mut Vec<ArrayRef>,
    schema_vec: &mut Vec<Field>,
    proj_schema: SchemaRef,
) {
    let mut field_name = "";
    for field in proj_schema.fields() {
        if let Some(field_id) = field.metadata().unwrap().get(FIELD_ID) {
            let id = u32::from_str(field_id).unwrap();
            if id == fid {
                field_name = field.name();
            }
        }
    }
    match array_type {
        ArrayType::U64(v) => {
            let record_array = UInt64Array::from(v);
            batch_array_vec.push(Arc::new(record_array));
            schema_vec.push(Field::new(field_name, DataType::UInt64, false));
        }
        ArrayType::I64(v) => {
            let record_array = Int64Array::from(v);
            batch_array_vec.push(Arc::new(record_array));
            schema_vec.push(Field::new(field_name, DataType::Int64, false));
        }
        ArrayType::Str(v) => {
            let record_array = StringArray::from(v);
            batch_array_vec.push(Arc::new(record_array));
            schema_vec.push(Field::new(field_name, DataType::Utf8, false));
        }
        ArrayType::F64(v) => {
            let record_array = Float64Array::from(v);
            batch_array_vec.push(Arc::new(record_array));
            schema_vec.push(Field::new(field_name, DataType::Float64, false));
        }
        ArrayType::Bool(v) => {
            let record_array = BooleanArray::from(v);
            batch_array_vec.push(Arc::new(record_array));
            schema_vec.push(Field::new(field_name, DataType::Boolean, false));
        }
    }
}

fn build_ts_array(field_block: &HashMap<u32, Vec<DataBlock>>) -> Vec<i64> {
    let mut ts_array = vec![];
    for field in field_block {
        for block in field.1 {
            ts_array.append(&mut block.ts().to_vec());
        }
    }
    if ts_array.is_empty() {
        return ts_array;
    }
    ts_array.sort();
    ts_array.dedup();
    ts_array
}

fn build_field_array(
    field_block: &HashMap<u32, Vec<DataBlock>>,
    ts_array: &Vec<i64>,
    field_array_map: &mut HashMap<u32, ArrayType>,
) {
    let mut ts_array_index: usize = 0;

    let mut field_array_index = HashMap::new();
    loop {
        for fields in field_block {
            if fields.1.is_empty() {
                continue;
            }
            let entry = match fields.1[0] {
                DataBlock::U64 { .. } => field_array_map
                    .entry(*fields.0)
                    .or_insert(ArrayType::U64(vec![])),
                DataBlock::I64 { .. } => field_array_map
                    .entry(*fields.0)
                    .or_insert(ArrayType::I64(vec![])),
                DataBlock::Str { .. } => field_array_map
                    .entry(*fields.0)
                    .or_insert(ArrayType::Str(vec![])),
                DataBlock::F64 { .. } => field_array_map
                    .entry(*fields.0)
                    .or_insert(ArrayType::F64(vec![])),
                DataBlock::Bool { .. } => field_array_map
                    .entry(*fields.0)
                    .or_insert(ArrayType::Bool(vec![])),
            };
            push_record_array(
                *fields.0,
                entry,
                &mut field_array_index,
                fields.1,
                ts_array,
                ts_array_index,
            );
        }
        ts_array_index += 1;
        if ts_array_index == ts_array.len() {
            break;
        }
    }
}

fn make_record_batch(
    db_name: &String,
    block_map: HashMap<SeriesId, HashMap<u32, Vec<DataBlock>>>,
    store_engine: EngineRef,
    proj_schema: SchemaRef,
) -> Vec<RecordBatch> {
    let mut data = vec![];
    for field_block in block_map.iter() {
        let mut batch_array_vec: Vec<ArrayRef> = vec![];
        let mut schema_vec = vec![];
        let mut field_array_map = HashMap::new();

        // build ts array
        let ts_array = build_ts_array(field_block.1);
        let len = ts_array.len();

        // build record array
        build_field_array(field_block.1, &ts_array, &mut field_array_map);

        make_field_col(
            field_array_map,
            proj_schema.clone(),
            &mut batch_array_vec,
            &mut schema_vec,
        );

        make_tag_col(
            db_name,
            store_engine.clone(),
            *field_block.0,
            len,
            &mut batch_array_vec,
            &mut schema_vec,
        );

        debug!("ts vec  {:?}", ts_array);
        make_time_col_if_need(
            proj_schema.clone(),
            ts_array,
            &mut batch_array_vec,
            &mut schema_vec,
        );
        debug!("schema_vec  {:?}", schema_vec);
        debug!("batch_array_vec {:?}", batch_array_vec);
        debug!("proj_schema {:?}", proj_schema);

        let mut cols = Vec::with_capacity(schema_vec.len());
        for proj_field in proj_schema.fields() {
            for (index, field) in schema_vec.iter().enumerate() {
                if field.name() == proj_field.name() {
                    cols.push(batch_array_vec[index].clone());
                }
            }
        }
        if let Ok(record_batch) = RecordBatch::try_new(proj_schema.clone(), cols) {
            data.push(record_batch);
        } else {
            panic!("failed make record batch");
        }
    }
    data
}

fn make_field_col(
    field_array_map: HashMap<u32, ArrayType>,
    proj_schema: SchemaRef,
    batch_array_vec: &mut Vec<ArrayRef>,
    schema_vec: &mut Vec<Field>,
) {
    for i in field_array_map {
        push_record_batch(i.0, i.1, batch_array_vec, schema_vec, proj_schema.clone());
    }
}

fn make_time_col_if_need(
    proj_schema: SchemaRef,
    ts_array: Vec<i64>,
    batch_array_vec: &mut Vec<ArrayRef>,
    schema_vec: &mut Vec<Field>,
) {
    for field in proj_schema.fields() {
        if field.name() == TIME_FIELD {
            let ts_record_array = Int64Array::from(ts_array);
            batch_array_vec.push(Arc::new(ts_record_array));
            schema_vec.push(Field::new(TIME_FIELD, DataType::Int64, false));
            break;
        }
    }
}

fn make_tag_col(
    db_name: &String,
    store_engine: EngineRef,
    sid: SeriesId,
    len: usize,
    batch_array_vec: &mut Vec<ArrayRef>,
    schema_vec: &mut Vec<Field>,
) {
    let res = store_engine.get_series_key(db_name, sid);
    match res {
        Ok(v) => {
            if let Some(series_key) = v {
                let tags = series_key.tags();
                for i in tags {
                    let mut tag_value_vec = vec![];
                    for _ in 0..len {
                        tag_value_vec.push(String::from_utf8(i.value.clone()).unwrap());
                    }
                    let record_array = StringArray::from(tag_value_vec);
                    batch_array_vec.push(Arc::new(record_array));
                    schema_vec.push(Field::new(
                        String::from_utf8(i.key.clone()).unwrap().as_str(),
                        DataType::Utf8,
                        false,
                    ));
                }
            }
        }
        Err(e) => {
            error!("error : {:?}", e);
        }
    }
}
