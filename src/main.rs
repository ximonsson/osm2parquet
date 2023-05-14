use parquet::{
    data_type::{ByteArray, ByteArrayType, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};

fn pqwriter<W: std::io::Write>(
    msgtype: &str,
    f: W,
) -> parquet::errors::Result<SerializedFileWriter<W>> {
    let schema = std::sync::Arc::new(parse_message_type(msgtype).unwrap());
    let props = std::sync::Arc::new(WriterProperties::builder().build());
    SerializedFileWriter::new(f, schema, props)
}

fn tags2parquet(elements: &Vec<impl osm::Element>, fp: &str) {
    // create data frame
    let ks: Vec<ByteArray> = elements
        .iter()
        .map(|e| {
            e.tags()
                .iter()
                .map(|t| -> ByteArray { t.k.as_str().into() })
                .collect::<Vec<ByteArray>>()
        })
        .flatten()
        .collect::<Vec<ByteArray>>();

    let vs: Vec<ByteArray> = elements
        .iter()
        .map(|e| {
            e.tags()
                .iter()
                .map(|t| -> ByteArray { t.v.as_str().into() })
                .collect::<Vec<ByteArray>>()
        })
        .flatten()
        .collect::<Vec<ByteArray>>();

    let ids = elements
        .iter()
        .map(|e| std::iter::repeat(e.id()).take(e.tags().len()))
        .flatten()
        .collect::<Vec<i64>>();

    let msgtype = "
        message schema {
            REQUIRED INT64 id;
            REQUIRED BYTE_ARRAY k;
            REQUIRED BYTE_ARRAY v;
        }
        ";

    let mut w = pqwriter(msgtype, std::fs::File::create(fp).unwrap()).unwrap();
    let mut rgw = w.next_row_group().unwrap();

    for i in 0..3 {
        if let Some(mut cw) = rgw.next_column().unwrap() {
            // write data
            let res = match i {
                0 => cw.typed::<Int64Type>().write_batch(&ids, None, None),
                1 => cw.typed::<ByteArrayType>().write_batch(&ks, None, None),
                2 => cw.typed::<ByteArrayType>().write_batch(&vs, None, None),
                _ => panic!("should not happen"),
            };
            res.unwrap();
            cw.close().unwrap();
        }
    }

    rgw.close().unwrap();
    w.close().unwrap();
}

fn nodes2parquet(data: &osm::File, dst: &str) {
    //
    // store nodes
    //

    let mut ids = Vec::<i64>::with_capacity(data.nodes.len());
    let mut lat = Vec::<f64>::with_capacity(data.nodes.len());
    let mut lon = Vec::<f64>::with_capacity(data.nodes.len());
    data.nodes.iter().for_each(|n| {
        ids.push(n.id);
        lat.push(n.lat);
        lon.push(n.lon)
    });

    let msgtype = "
        message schema {
            REQUIRED INT64 id;
            REQUIRED DOUBLE lat;
            REQUIRED DOUBLE lon;
        }
        ";

    let mut w = pqwriter::<std::fs::File>(
        msgtype,
        std::fs::File::create(format!("{}/nodes.parquet", dst)).unwrap(),
    )
    .unwrap();
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
    w.close().unwrap();

    //
    // store tags
    //

    println!(" >> tags.");
    tags2parquet(&data.nodes, &format!("{}/node-tags.parquet", dst));
}

fn ways2parquet(data: &osm::File, dst: &str) {
    //
    // store ways
    //

    let ids = data.ways.iter().map(|w| w.id).collect::<Vec<i64>>();

    let msgtype = "
        message schema {
            REQUIRED INT64 id;
        }
        ";

    let mut w = pqwriter::<std::fs::File>(
        msgtype,
        std::fs::File::create(format!("{}/ways.parquet", dst)).unwrap(),
    )
    .unwrap();
    let mut rgw = w.next_row_group().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    cw.close().unwrap();

    //
    // store tags
    //

    println!(" >> tags.");
    tags2parquet(&data.ways, &format!("{}/way-tags.parquet", dst));

    //
    // store node refs
    //

    println!(" >> node refs.");
    let ids = data
        .ways
        .iter()
        .map(|w| std::iter::repeat(w.id).take(w.nodes.len()))
        .flatten()
        .collect::<Vec<i64>>();

    let refs = data
        .ways
        .iter()
        .map(|w| w.nodes.iter().map(|n| n.r#ref).collect::<Vec<i64>>())
        .flatten()
        .collect::<Vec<i64>>();

    let msgtype = "
        message schema {
            REQUIRED INT64 way;
            REQUIRED INT64 node;
        }
        ";

    let mut w = pqwriter::<std::fs::File>(
        msgtype,
        std::fs::File::create(format!("{}/ways.parquet", dst)).unwrap(),
    )
    .unwrap();
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
}

fn rels2parquet(data: &osm::File, dst: &str) {
    //
    // store relations
    //

    let ids = data.relations.iter().map(|r| r.id).collect::<Vec<i64>>();

    let msgtype = "
        message schema {
            REQUIRED INT64 id;
        }
        ";

    let mut w = pqwriter::<std::fs::File>(
        msgtype,
        std::fs::File::create(format!("{}/relations.parquet", dst)).unwrap(),
    )
    .unwrap();
    let mut rgw = w.next_row_group().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    cw.close().unwrap();

    //
    // store tags
    //

    println!(" >> tags.");
    tags2parquet(&data.relations, &format!("{}/relation-tags.parquet", dst));

    //
    // store members
    //

    println!(" >> members.");
    let memid = data
        .relations
        .iter()
        .map(|r| r.members.iter().map(|m| m.r#ref).collect::<Vec<i64>>())
        .flatten()
        .collect::<Vec<i64>>();

    let memtype = data
        .relations
        .iter()
        .map(|r| {
            r.members
                .iter()
                .map(|m| -> ByteArray { m.r#type.as_str().into() })
                .collect::<Vec<ByteArray>>()
        })
        .flatten()
        .collect::<Vec<ByteArray>>();

    let memrole = data
        .relations
        .iter()
        .map(|r| {
            r.members
                .iter()
                .map(|m| -> ByteArray { m.role.as_str().into() })
                .collect::<Vec<ByteArray>>()
        })
        .flatten()
        .collect::<Vec<ByteArray>>();

    let ids = data
        .relations
        .iter()
        .map(|e| std::iter::repeat(e.id).take(e.members.len()))
        .flatten()
        .collect::<Vec<i64>>();

    let msgtype = "
        message schema {
            REQUIRED INT64 relation;
            REQUIRED INT64 member;
            REQUIRED BYTE_ARRAY role;
            REQUIRED BYTE_ARRAY type;
        }
        ";

    let mut w = pqwriter::<std::fs::File>(
        msgtype,
        std::fs::File::create(format!("{}/relation-members.parquet", dst)).unwrap(),
    )
    .unwrap();
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
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 3 {
        eprintln!("not enought arguments; usage:");
        eprintln!("osm2parquet [inputfile] [target directory]");
        std::process::exit(1);
    }

    let inputfile = &args[1];
    let target = &args[2];

    //
    // load osm data
    //

    println!("load osm data. ---");
    println!(" > file: {}", inputfile);

    let f = std::fs::File::open(inputfile).unwrap();
    let data = osm::File::from_proto_reader(f).unwrap();

    println!(
        " > {} nodes, {} ways & {} relations loaded",
        data.nodes.len(),
        data.ways.len(),
        data.relations.len()
    );

    //
    // migrate to duckdb
    //

    println!("migrate to parquet. ---");

    // populate database
    println!(" > nodes.");
    nodes2parquet(&data, &target);

    println!(" > ways.");
    ways2parquet(&data, &target);

    println!(" > relations.");
    rels2parquet(&data, &target);

    println!("done.")
}
