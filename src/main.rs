mod pq;

use std::fs::File;

fn export_pbf(r: impl std::io::Read + 'static, dst: &str) {
    //
    // node writers
    //

    let mut wnode = pq::writer(
        pq::SCHEMA_NODE,
        File::create(format!("{}/nodes.parquet", dst)).unwrap(),
    )
    .unwrap();

    let mut wnode_tags = pq::writer(
        pq::SCHEMA_TAGS,
        File::create(format!("{}/node-tags.parquet", dst)).unwrap(),
    )
    .unwrap();

    //
    // way writers
    //

    let mut wway = pq::writer(
        pq::SCHEMA_WAY,
        File::create(format!("{}/ways.parquet", dst)).unwrap(),
    )
    .unwrap();

    let mut wway_tags = pq::writer(
        pq::SCHEMA_TAGS,
        File::create(format!("{}/way-tags.parquet", dst)).unwrap(),
    )
    .unwrap();

    let mut wway_nodes = pq::writer(
        pq::SCHEMA_WAY_NODE,
        File::create(format!("{}/way-nodes.parquet", dst)).unwrap(),
    )
    .unwrap();

    //
    // relations
    //

    let mut wrel = pq::writer(
        pq::SCHEMA_RELATION,
        File::create(format!("{}/relations.parquet", dst)).unwrap(),
    )
    .unwrap();

    let mut wrel_tags = pq::writer(
        pq::SCHEMA_TAGS,
        File::create(format!("{}/relation-tags.parquet", dst)).unwrap(),
    )
    .unwrap();

    let mut wrel_mem = pq::writer(
        pq::SCHEMA_RELATION_MEMBER,
        File::create(format!("{}/relation-members.parquet", dst)).unwrap(),
    )
    .unwrap();

    //
    // Worker threads for each primitive element
    //

    const BUFSIZE: usize = 1000000;
    const CHANSIZE: usize = 500;

    println!("start workers.");

    // Nodes
    let (sender_node, receiver_node) = std::sync::mpsc::sync_channel(CHANSIZE);
    let worker_nodes = std::thread::spawn(move || {
        let mut buf = Vec::<osm::Node>::with_capacity(BUFSIZE);

        while let Ok(mut vs) = receiver_node.recv() {
            buf.append(&mut vs);
            if buf.len() >= BUFSIZE {
                pq::write_nodes(&buf, &mut wnode);
                pq::write_tags(&buf, &mut wnode_tags);
                buf.clear();
            }
        }

        pq::write_nodes(&buf, &mut wnode);
        pq::write_tags(&buf, &mut wnode_tags);
        buf.clear();
        wnode.close().unwrap();
        wnode_tags.close().unwrap();
    });

    // Ways
    let (sender_of_ways, receiver_of_ways) = std::sync::mpsc::sync_channel(CHANSIZE);
    let worker_ways = std::thread::spawn(move || {
        let mut buf = Vec::<osm::Way>::with_capacity(BUFSIZE);

        while let Ok(mut vs) = receiver_of_ways.recv() {
            buf.append(&mut vs);
            if buf.len() >= BUFSIZE {
                pq::write_ways(&buf, &mut wway);
                pq::write_tags(&buf, &mut wway_tags);
                pq::write_way_nodes(&buf, &mut wway_nodes);
                buf.clear();
            }
        }

        pq::write_ways(&buf, &mut wway);
        pq::write_tags(&buf, &mut wway_tags);
        pq::write_way_nodes(&buf, &mut wway_nodes);
        buf.clear();

        wway.close().unwrap();
        wway_tags.close().unwrap();
        wway_nodes.close().unwrap();
    });

    // Relations
    let (sender_of_relations, receiver_of_relations) = std::sync::mpsc::sync_channel(CHANSIZE);
    let worker_relations = std::thread::spawn(move || {
        let mut buf = Vec::<osm::Relation>::with_capacity(BUFSIZE);

        while let Ok(mut vs) = receiver_of_relations.recv() {
            buf.append(&mut vs);
            if buf.len() >= BUFSIZE {
                pq::write_relations(&buf, &mut wrel);
                pq::write_tags(&buf, &mut wrel_tags);
                pq::write_relation_members(&buf, &mut wrel_mem);
                buf.clear();
            }
        }

        pq::write_relations(&buf, &mut wrel);
        pq::write_tags(&buf, &mut wrel_tags);
        pq::write_relation_members(&buf, &mut wrel_mem);
        buf.clear();

        wrel.close().unwrap();
        wrel_tags.close().unwrap();
        wrel_mem.close().unwrap();
    });

    for fb in osm::proto::FileBlockIterator::from_reader(r) {
        if let osm::proto::FileBlock::Primitive(b) = fb {
            let str_tbl = osm::proto::parse_str_tbl(&b);
            for pg in &b.primitivegroup {
                if let Some(dense) = &pg.dense {
                    sender_node
                        .send(
                            osm::Node::from_proto_dense_nodes(&dense, &str_tbl, &b)
                                .collect::<Vec<osm::Node>>(),
                        )
                        .unwrap();
                } else if pg.ways.len() > 0 {
                    sender_of_ways
                        .send(
                            pg.ways
                                .iter()
                                .map(|way| osm::Way::from_proto(&way, &str_tbl))
                                .collect::<Vec<osm::Way>>(),
                        )
                        .unwrap();
                } else if pg.relations.len() > 0 {
                    sender_of_relations
                        .send(
                            pg.relations
                                .iter()
                                .map(|rel| osm::Relation::from_proto(&rel, &str_tbl))
                                .collect::<Vec<osm::Relation>>(),
                        )
                        .unwrap();
                } else if pg.nodes.len() > 0 {
                    sender_node
                        .send(
                            pg.nodes
                                .iter()
                                .map(|node| osm::Node::from_proto(&node, &str_tbl, &b))
                                .collect::<Vec<osm::Node>>(),
                        )
                        .unwrap();
                }
            }
        }
    }

    // Join workers
    println!("join threads");

    drop(sender_node);
    drop(sender_of_relations);
    drop(sender_of_ways);

    worker_nodes.join().unwrap();
    worker_ways.join().unwrap();
    worker_relations.join().unwrap();
}

fn tags2parquet(elements: &[impl osm::Element], fp: &str) {
    let mut w = pq::writer(pq::SCHEMA_TAGS, File::create(fp).unwrap()).unwrap();
    pq::write_tags(elements, &mut w);

    w.close().unwrap();
}

fn nodes2parquet(nodes: &[osm::Node], dst: &str) {
    //
    // store nodes
    //

    let mut w = pq::writer(
        pq::SCHEMA_NODE,
        File::create(format!("{}/nodes.parquet", dst)).unwrap(),
    )
    .unwrap();
    pq::write_nodes(nodes, &mut w);
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

    let mut w = pq::writer(
        pq::SCHEMA_RELATION,
        File::create(format!("{}/relations.parquet", dst)).unwrap(),
    )
    .unwrap();
    pq::write_relations(relations, &mut w);
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

    let mut w = pq::writer(
        pq::SCHEMA_RELATION_MEMBER,
        File::create(format!("{}/relation-members.parquet", dst)).unwrap(),
    )
    .unwrap();
    pq::write_relation_members(relations, &mut w);
    w.close().unwrap();
}

fn ways2parquet(ways: &[osm::Way], dst: &str) {
    //
    // store ways
    //

    let mut w = pq::writer(
        pq::SCHEMA_WAY,
        File::create(format!("{}/ways.parquet", dst)).unwrap(),
    )
    .unwrap();
    pq::write_ways(ways, &mut w);
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

    let mut w = pq::writer(
        pq::SCHEMA_WAY_NODE,
        File::create(format!("{}/way-nodes.parquet", dst)).unwrap(),
    )
    .unwrap();
    pq::write_way_nodes(ways, &mut w);

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

    println!("export to parquet. ---");

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

    println!("--- export data {} ==> {} ---", inputfile, target);

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
