use parquet::{
    data_type::{ByteArray, ByteArrayType, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};

pub const SCHEMA_TAGS: &str = "message schema {
    REQUIRED INT64 id;
    REQUIRED BYTE_ARRAY k;
    REQUIRED BYTE_ARRAY v;
}";

pub const SCHEMA_NODE: &str = "message schema {
    REQUIRED INT64 id;
    REQUIRED DOUBLE lat;
    REQUIRED DOUBLE lon;
}";

pub const SCHEMA_WAY: &str = "message schema {
    REQUIRED INT64 id;
}";

pub const SCHEMA_WAY_NODE: &str = "message schema {
    REQUIRED INT64 way;
    REQUIRED INT64 node;
}";

pub const SCHEMA_RELATION: &str = "message schema {
    REQUIRED INT64 id;
}";

pub const SCHEMA_RELATION_MEMBER: &str = "message schema {
    REQUIRED INT64 relation;
    REQUIRED INT64 member;
    REQUIRED BYTE_ARRAY role;
    REQUIRED BYTE_ARRAY type;
}";

pub fn writer<W: std::io::Write>(
    msgtype: &str,
    f: W,
) -> parquet::errors::Result<SerializedFileWriter<W>> {
    let schema = std::sync::Arc::new(parse_message_type(msgtype).unwrap());
    let props = std::sync::Arc::new(
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::SNAPPY)
            .build(),
    );
    SerializedFileWriter::new(f, schema, props)
}

pub fn write_tags<W: std::io::Write>(
    elements: &[impl osm::Element],
    w: &mut SerializedFileWriter<W>,
) {
    let ks: Vec<ByteArray> = elements
        .iter()
        .map(|e| {
            e.tags()
                .iter()
                .map(|t| t.k.as_str().into())
                .collect::<Vec<ByteArray>>()
        })
        .flatten()
        .collect();

    let vs: Vec<ByteArray> = elements
        .iter()
        .map(|e| {
            e.tags()
                .iter()
                .map(|t| t.v.as_str().into())
                .collect::<Vec<ByteArray>>()
        })
        .flatten()
        .collect();

    let ids: Vec<i64> = elements
        .iter()
        .map(|e| std::iter::repeat(e.id()).take(e.tags().len()))
        .flatten()
        .collect();

    let mut rgw = w.next_row_group().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    cw.close().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<ByteArrayType>()
        .write_batch(&ks, None, None)
        .unwrap();
    cw.close().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<ByteArrayType>()
        .write_batch(&vs, None, None)
        .unwrap();
    cw.close().unwrap();

    rgw.close().unwrap();
}

pub fn write_nodes<W: std::io::Write>(nodes: &[osm::Node], w: &mut SerializedFileWriter<W>) {
    let mut ids = Vec::<i64>::with_capacity(nodes.len());
    let mut lat = Vec::<f64>::with_capacity(nodes.len());
    let mut lon = Vec::<f64>::with_capacity(nodes.len());
    // divide into groups so we don't kill the machine
    nodes.iter().for_each(|n| {
        ids.push(n.id);
        lat.push(n.lat);
        lon.push(n.lon)
    });

    let mut rgw = w.next_row_group().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    cw.close().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<parquet::data_type::DoubleType>()
        .write_batch(&lat, None, None)
        .unwrap();
    cw.close().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<parquet::data_type::DoubleType>()
        .write_batch(&lon, None, None)
        .unwrap();
    cw.close().unwrap();

    rgw.close().unwrap();
    ids.clear();
    lat.clear();
    lon.clear();
}

pub fn write_ways<W: std::io::Write>(ways: &[osm::Way], w: &mut SerializedFileWriter<W>) {
    let ids: Vec<i64> = ways.iter().map(|w| w.id).collect();
    let mut rgw = w.next_row_group().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    cw.close().unwrap();

    rgw.close().unwrap();
}

pub fn write_way_nodes<W: std::io::Write>(ways: &[osm::Way], w: &mut SerializedFileWriter<W>) {
    let ids: Vec<i64> = ways
        .iter()
        .map(|w| std::iter::repeat(w.id).take(w.nodes.len()))
        .flatten()
        .collect();

    let refs: Vec<i64> = ways
        .iter()
        .map(|w| w.nodes.iter().map(|n| n.r#ref).collect::<Vec<i64>>())
        .flatten()
        .collect();

    let mut rgw = w.next_row_group().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    cw.close().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&refs, None, None)
        .unwrap();
    cw.close().unwrap();

    rgw.close().unwrap();
}

pub fn write_relations<W: std::io::Write>(
    relations: &[osm::Relation],
    w: &mut SerializedFileWriter<W>,
) {
    let ids: Vec<i64> = relations.iter().map(|r| r.id).collect();

    let mut rgw = w.next_row_group().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    cw.close().unwrap();

    rgw.close().unwrap();
}

pub fn write_relation_members<W: std::io::Write>(
    relations: &[osm::Relation],
    w: &mut SerializedFileWriter<W>,
) {
    let memid: Vec<i64> = relations
        .iter()
        .map(|r| r.members.iter().map(|m| m.r#ref).collect::<Vec<i64>>())
        .flatten()
        .collect();

    let memtype: Vec<ByteArray> = relations
        .iter()
        .map(|r| {
            r.members
                .iter()
                .map(|m| m.r#type.as_str().into())
                .collect::<Vec<ByteArray>>()
        })
        .flatten()
        .collect();

    let memrole: Vec<ByteArray> = relations
        .iter()
        .map(|r| {
            r.members
                .iter()
                .map(|m| m.role.as_str().into())
                .collect::<Vec<ByteArray>>()
        })
        .flatten()
        .collect();

    let ids: Vec<i64> = relations
        .iter()
        .map(|e| std::iter::repeat(e.id).take(e.members.len()))
        .flatten()
        .collect();

    let mut rgw = w.next_row_group().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    cw.close().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&memid, None, None)
        .unwrap();
    cw.close().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<ByteArrayType>()
        .write_batch(&memrole, None, None)
        .unwrap();
    cw.close().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<ByteArrayType>()
        .write_batch(&memtype, None, None)
        .unwrap();
    cw.close().unwrap();

    rgw.close().unwrap();
}
