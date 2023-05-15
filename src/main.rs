use parquet::{
    data_type::{ByteArray, ByteArrayType, Int64Type},
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::parser::parse_message_type,
};

use std::fs::File;

fn pqwriter<W: std::io::Write>(
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

fn write_tags<W: std::io::Write>(elements: &[impl osm::Element], w: &mut SerializedFileWriter<W>) {
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

fn write_nodes<W: std::io::Write>(nodes: &[osm::Node], w: &mut SerializedFileWriter<W>) {
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

fn write_ways<W: std::io::Write>(ways: &[osm::Way], w: &mut SerializedFileWriter<W>) {
    let ids: Vec<i64> = ways.iter().map(|w| w.id).collect();
    let mut rgw = w.next_row_group().unwrap();

    let mut cw = rgw.next_column().unwrap().unwrap();
    cw.typed::<Int64Type>()
        .write_batch(&ids, None, None)
        .unwrap();
    cw.close().unwrap();

    rgw.close().unwrap();
}

fn write_way_nodes<W: std::io::Write>(ways: &[osm::Way], w: &mut SerializedFileWriter<W>) {
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

fn write_relations<W: std::io::Write>(
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

fn write_relation_members<W: std::io::Write>(
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

fn export_pbf(r: impl std::io::Read + 'static, dst: &str) {
    //
    // node writers
    //

    let mut wnode = pqwriter(
        "message schema {
            REQUIRED INT64 id;
            REQUIRED DOUBLE lat;
            REQUIRED DOUBLE lon;
        }
        ",
        File::create(format!("{}/nodes.parquet", dst)).unwrap(),
    )
    .unwrap();

    let mut wnode_tags = pqwriter(
        "message schema {
            REQUIRED INT64 id;
            REQUIRED BYTE_ARRAY k;
            REQUIRED BYTE_ARRAY v;
        }",
        File::create(format!("{}/node-tags.parquet", dst)).unwrap(),
    )
    .unwrap();

    //
    // way writers
    //

    let mut wway = pqwriter(
        "message schema {
            REQUIRED INT64 id;
        }",
        File::create(format!("{}/ways.parquet", dst)).unwrap(),
    )
    .unwrap();

    let mut wway_tags = pqwriter(
        "message schema {
            REQUIRED INT64 id;
            REQUIRED BYTE_ARRAY k;
            REQUIRED BYTE_ARRAY v;
        }",
        File::create(format!("{}/way-tags.parquet", dst)).unwrap(),
    )
    .unwrap();

    let mut wway_nodes = pqwriter(
        "message schema {
            REQUIRED INT64 way;
            REQUIRED INT64 node;
        }",
        File::create(format!("{}/way-nodes.parquet", dst)).unwrap(),
    )
    .unwrap();

    //
    // relations
    //

    let mut wrel = pqwriter(
        "message schema {
            REQUIRED INT64 id;
        }",
        File::create(format!("{}/relations.parquet", dst)).unwrap(),
    )
    .unwrap();

    let mut wrel_tags = pqwriter(
        "message schema {
            REQUIRED INT64 id;
            REQUIRED BYTE_ARRAY k;
            REQUIRED BYTE_ARRAY v;
        }",
        File::create(format!("{}/relation-tags.parquet", dst)).unwrap(),
    )
    .unwrap();

    let mut wrel_mem = pqwriter(
        "message schema {
            REQUIRED INT64 relation;
            REQUIRED INT64 member;
            REQUIRED BYTE_ARRAY role;
            REQUIRED BYTE_ARRAY type;
        }
        ",
        File::create(format!("{}/relation-members.parquet", dst)).unwrap(),
    )
    .unwrap();

    //
    // buffers
    //

    const BUFSIZE: usize = 1000000;

    let mut nodes = Vec::<osm::Node>::with_capacity(BUFSIZE);
    let mut ways = Vec::<osm::Way>::with_capacity(BUFSIZE);
    let mut rels = Vec::<osm::Relation>::with_capacity(BUFSIZE);

    for fb in osm::proto::FileBlockIterator::from_reader(r) {
        if let osm::proto::FileBlock::Primitive(b) = fb {
            let str_tbl = osm::proto::parse_str_tbl(&b);
            for pg in &b.primitivegroup {
                if let Some(dense) = &pg.dense {
                    nodes.append(
                        &mut osm::Node::from_proto_dense_nodes(&dense, &str_tbl, &b)
                            .collect::<Vec<osm::Node>>(),
                    );

                    if nodes.len() >= BUFSIZE {
                        write_nodes(&nodes, &mut wnode);
                        write_tags(&nodes, &mut wnode_tags);
                        nodes.clear();
                    }
                } else if pg.ways.len() > 0 {
                    ways.append(
                        &mut pg
                            .ways
                            .iter()
                            .map(|way| osm::Way::from_proto(&way, &str_tbl))
                            .collect::<Vec<osm::Way>>(),
                    );

                    if ways.len() >= BUFSIZE {
                        write_ways(&ways, &mut wway);
                        write_tags(&ways, &mut wway_tags);
                        write_way_nodes(&ways, &mut wway_nodes);
                        ways.clear();
                    }
                } else if pg.relations.len() > 0 {
                    rels.append(
                        &mut pg
                            .relations
                            .iter()
                            .map(|rel| osm::Relation::from_proto(&rel, &str_tbl))
                            .collect::<Vec<osm::Relation>>(),
                    );

                    if rels.len() >= BUFSIZE {
                        write_relations(&rels, &mut wrel);
                        write_tags(&rels, &mut wrel_tags);
                        write_relation_members(&rels, &mut wrel_mem);
                        rels.clear();
                    }
                } else if pg.nodes.len() > 0 {
                    nodes.append(
                        &mut pg
                            .nodes
                            .iter()
                            .map(|node| osm::Node::from_proto(&node, &str_tbl, &b))
                            .collect::<Vec<osm::Node>>(),
                    );

                    if nodes.len() >= BUFSIZE {
                        write_nodes(&nodes, &mut wnode);
                        write_tags(&nodes, &mut wnode_tags);
                        nodes.clear();
                    }
                }
            }
        }
    }

    // flush buffers
    write_nodes(&nodes, &mut wnode);
    write_tags(&nodes, &mut wnode_tags);
    write_ways(&ways, &mut wway);
    write_tags(&ways, &mut wway_tags);
    write_way_nodes(&ways, &mut wway_nodes);
    write_relations(&rels, &mut wrel);
    write_tags(&rels, &mut wrel_tags);
    write_relation_members(&rels, &mut wrel_mem);

    // close all writers
    wnode.close().unwrap();
    wnode_tags.close().unwrap();
    wway.close().unwrap();
    wway_tags.close().unwrap();
    wway_nodes.close().unwrap();
    wrel.close().unwrap();
    wrel_tags.close().unwrap();
    wrel_mem.close().unwrap();
}

fn tags2parquet(elements: &[impl osm::Element], fp: &str) {
    let mut w = pqwriter(
        "message schema {
            REQUIRED INT64 id;
            REQUIRED BYTE_ARRAY k;
            REQUIRED BYTE_ARRAY v;
        }",
        File::create(fp).unwrap(),
    )
    .unwrap();
    write_tags(elements, &mut w);

    w.close().unwrap();
}

fn nodes2parquet(nodes: &[osm::Node], dst: &str) {
    //
    // store nodes
    //

    let mut w = pqwriter(
        "message schema {
            REQUIRED INT64 id;
            REQUIRED DOUBLE lat;
            REQUIRED DOUBLE lon;
        }
        ",
        File::create(format!("{}/nodes.parquet", dst)).unwrap(),
    )
    .unwrap();
    write_nodes(nodes, &mut w);
    w.close().unwrap();

    //
    // store tags
    //

    println!(" >> tags.");
    tags2parquet(nodes, &format!("{}/node-tags.parquet", dst));
}

fn rels2parquet(relations: &[osm::Relation], dst: &str) {
    //
    // store relations
    //

    let mut w = pqwriter(
        "message schema {
            REQUIRED INT64 id;
        }",
        File::create(format!("{}/relations.parquet", dst)).unwrap(),
    )
    .unwrap();
    write_relations(relations, &mut w);
    w.close().unwrap();

    //
    // store tags
    //

    println!(" >> tags.");
    tags2parquet(relations, &format!("{}/relation-tags.parquet", dst));

    //
    // store members
    //

    println!(" >> members.");

    let mut w = pqwriter(
        "message schema {
            REQUIRED INT64 relation;
            REQUIRED INT64 member;
            REQUIRED BYTE_ARRAY role;
            REQUIRED BYTE_ARRAY type;
        }
        ",
        File::create(format!("{}/relation-members.parquet", dst)).unwrap(),
    )
    .unwrap();
    write_relation_members(relations, &mut w);
    w.close().unwrap();
}

fn ways2parquet(ways: &[osm::Way], dst: &str) {
    //
    // store ways
    //

    let mut w = pqwriter(
        "message schema {
            REQUIRED INT64 id;
        }",
        File::create(format!("{}/ways.parquet", dst)).unwrap(),
    )
    .unwrap();
    write_ways(ways, &mut w);
    w.close().unwrap();

    //
    // store tags
    //

    println!(" >> tags.");
    tags2parquet(ways, &format!("{}/way-tags.parquet", dst));

    //
    // store node refs
    //

    println!(" >> node refs.");

    let mut w = pqwriter(
        "message schema {
            REQUIRED INT64 way;
            REQUIRED INT64 node;
        }",
        File::create(format!("{}/way-nodes.parquet", dst)).unwrap(),
    )
    .unwrap();
    write_way_nodes(ways, &mut w);

    w.close().unwrap();
}

fn export_xml(r: impl std::io::Read, dst: &str) {
    println!("load osm data. ---");

    let data = osm::File::from_reader(r).unwrap();

    println!(
        " > {} nodes, {} ways & {} relations loaded",
        data.nodes.len(),
        data.ways.len(),
        data.relations.len()
    );

    //
    // migrate to parquet
    //

    println!("migrate to parquet. ---");

    // populate database
    println!(" > nodes.");
    nodes2parquet(&data.nodes, &dst);

    println!(" > ways.");
    ways2parquet(&data.ways, &dst);

    println!(" > relations.");
    rels2parquet(&data.relations, &dst);
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

    println!("=== Export OSM data from {} => {} ===", inputfile, target);

    let r = File::open(inputfile).unwrap();
    match std::path::Path::new(inputfile)
        .extension()
        .unwrap()
        .to_str()
    {
        Some("pbf") => export_pbf(r, target),
        Some("osm") | Some("xml") => export_xml(r, target),
        Some(x) => panic!("Unrecognized file extension {}!", x),
        None => panic!("Unrecognized file extension!"),
    };

    println!("done.")
}
