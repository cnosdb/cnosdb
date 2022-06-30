use protos::models as fb_models;

use crate::{
    field_info, generate_series_id, Error, FieldID, Result, SeriesID, Tag, Timestamp, ValueType,
};

#[derive(Debug)]
pub struct FieldValue {
    pub field_id: FieldID,
    pub value_type: ValueType,
    pub value: Vec<u8>,
}

impl FieldValue {
    pub fn from_flatbuffers(field: &fb_models::Field) -> Result<Self> {
        Ok(Self { field_id: 0,
                  value_type: field.type_().into(),
                  value: match field.value() {
                      Some(v) => v.to_vec(),
                      None => Vec::new(),
                  } })
    }

    pub fn field_id(&self) -> FieldID {
        self.field_id
    }
}

#[derive(Debug)]
pub struct InMemPoint {
    pub series_id: SeriesID,
    pub timestamp: Timestamp,
    pub fields: Vec<FieldValue>,
}

impl InMemPoint {
    pub fn from_flatbuffers(point: &fb_models::Point) -> Result<Self> {
        let fields = match point.fields() {
            Some(fields_inner) => {
                let mut fields = Vec::with_capacity(fields_inner.len());
                for f in fields_inner.into_iter() {
                    fields.push(FieldValue::from_flatbuffers(&f)?);
                }
                fields
            },
            None => return Err(Error::InvalidFlatbufferMessage { err: "Point fields cannot be empty".to_string() }),
        };
        Ok(Self { series_id: 0, timestamp: point.timestamp(), fields })
    }

    pub fn series_id(&self) -> SeriesID {
        self.series_id
    }

    pub fn fields(&self) -> &Vec<FieldValue> {
        &self.fields
    }
}

impl From<fb_models::Point<'_>> for InMemPoint {
    fn from(p: fb_models::Point<'_>) -> Self {
        let mut fields = Vec::new();
        let mut tags = Vec::new();

        for tit in p.tags().into_iter() {
            for t in tit.into_iter() {
                let k = t.key().unwrap().to_vec();
                let v = t.value().unwrap().to_vec();
                let tag = Tag::new(k, v);
                tags.push(tag);
            }
        }

        let sid = generate_series_id(&mut tags);

        for fit in p.fields().into_iter() {
            for f in fit.into_iter() {
                let field_name = f.name().unwrap().to_vec();
                let val_type = f.type_().into();
                let val = f.value().unwrap().to_vec();
                let fid = field_info::generate_field_id(&field_name, sid);
                fields.push(FieldValue { field_id: fid, value_type: val_type, value: val });
            }
        }
        let ts = p.timestamp();

        Self { series_id: sid, timestamp: ts, fields }
    }
}

#[cfg(test)]
mod test_points {
    use protos::models;

    use crate::InMemPoint;

    #[test]
    fn test_from() {
        let mut fb = flatbuffers::FlatBufferBuilder::new();

        // build tag
        let tag_k = fb.create_vector("tag_k".as_bytes());
        let tag_v = fb.create_vector("tag_v".as_bytes());
        let tag =
            models::Tag::create(&mut fb, &models::TagArgs { key: Some(tag_k), value: Some(tag_v) });
        // build field
        let f_n = fb.create_vector("field_name".as_bytes());
        let f_v = fb.create_vector("field_value".as_bytes());

        let field =
            models::Field::create(&mut fb,
                                  &models::FieldArgs { name: Some(f_n),
                                                       type_:
                                                           protos::models::FieldType::Integer,
                                                       value: Some(f_v) });
        // build series_info
        let fields = Some(fb.create_vector(&[field]));
        let tags = Some(fb.create_vector(&[tag]));
        // build point
        let point =
            models::Point::create(&mut fb, &models::PointArgs { tags, fields, timestamp: 1 });

        fb.finish(point, None);
        let buf = fb.finished_data();

        let p = flatbuffers::root::<models::Point>(buf).unwrap();
        println!("Point info {:?}", p);

        let s = InMemPoint::from(p);
        println!("Series info {:?}", s);
    }
}
