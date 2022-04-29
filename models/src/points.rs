use protos::models::Point;

use crate::{FieldID, FieldInfo, SeriesID, SeriesInfo, Tag, ValueType};

#[derive(Debug)]
pub struct AbstractPoint {
    pub filed_id: FieldID,
    pub val_type: ValueType,
    pub value: Vec<u8>,
}

impl AbstractPoint {
    pub fn filed_id(&self) -> FieldID {
        self.filed_id
    }
}

#[derive(Debug)]
pub struct AbstractPoints {
    pub series_id: SeriesID,
    pub timestamp: u64,
    pub fileds: Vec<AbstractPoint>,
}

impl AbstractPoints {
    pub fn series_id(&self) -> SeriesID {
        self.series_id
    }
    pub fn fileds(&self) -> &Vec<AbstractPoint> {
        &self.fileds
    }
}

impl From<Point<'_>> for AbstractPoints {
    fn from(p: Point<'_>) -> Self {
        let mut fileds = Vec::new();
        let mut tags = Vec::new();

        for tit in p.tags().into_iter() {
            for t in tit.into_iter() {
                let k = t.key().unwrap().to_vec();
                let v = t.value().unwrap().to_vec();
                let tag = Tag::new(k, v);
                tags.push(tag);
            }
        }

        let sid = SeriesInfo::cal_sid(&mut tags);

        for fit in p.fields().into_iter() {
            for f in fit.into_iter() {
                let field_name = f.name().unwrap().to_vec();
                let val_type = f.type_().into();
                let val = f.value().unwrap().to_vec();
                let fid = FieldInfo::cal_fid(&field_name, sid);
                fileds.push(AbstractPoint { filed_id: fid, val_type, value: val });
            }
        }
        let ts = p.timestamp();

        Self { series_id: sid, timestamp: ts, fileds }
    }
}

#[cfg(test)]
mod test_points {
    use protos::models;

    use crate::AbstractPoints;

    #[test]
    fn test_from() {
        let mut fb = flatbuffers::FlatBufferBuilder::new();

        // build tag
        let tag_k = fb.create_vector("tag_k".as_bytes());
        let tag_v = fb.create_vector("tag_v".as_bytes());
        let tag =
            models::Tag::create(&mut fb, &models::TagArgs { key: Some(tag_k), value: Some(tag_v) });
        // build filed
        let f_n = fb.create_vector("filed_name".as_bytes());
        let f_v = fb.create_vector("filed_value".as_bytes());

        let filed =
            models::Field::create(&mut fb,
                                  &models::FieldArgs { name: Some(f_n),
                                                       type_:
                                                           protos::models::FieldType::Integer,
                                                       value: Some(f_v) });
        // build series_info
        let fields = Some(fb.create_vector(&[filed]));
        let tags = Some(fb.create_vector(&[tag]));
        // build point
        let point =
            models::Point::create(&mut fb, &models::PointArgs { tags, fields, timestamp: 1 });

        fb.finish(point, None);
        let buf = fb.finished_data();

        let p = flatbuffers::root::<models::Point>(buf).unwrap();
        println!("Point info {:?}", p);

        let s = AbstractPoints::from(p);
        println!("Series info {:?}", s);
    }
}
