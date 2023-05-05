use polars::prelude::*;

fn tags2parquet(elements: &Vec<impl osm::Element>, fp: &str) {
    // create data frame
    let ks: Vec<&str> = elements
        .iter()
        .map(|e| e.tags().iter().map(|t| t.k.as_str()).collect::<Vec<&str>>())
        .flatten()
        .collect::<Vec<&str>>();

    let vs: Vec<&str> = elements
        .iter()
        .map(|e| e.tags().iter().map(|t| t.v.as_str()).collect::<Vec<&str>>())
        .flatten()
        .collect::<Vec<&str>>();

    let ids = elements
        .iter()
        .map(|e| std::iter::repeat(e.id()).take(e.tags().len()))
        .flatten()
        .collect::<Vec<i64>>();

    let mut df = df!("id" => ids, "k" => ks, "v" => vs).unwrap();
    let w = ParquetWriter::new(std::fs::File::create(fp).unwrap());
    w.finish(&mut df).unwrap();
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

    // create dataframe with the node info
    let mut df = df!("id" => &ids, "lat" => &lat, "lon" => &lon).unwrap();

    // store the dataframe to parquet
    let f = std::fs::File::create(format!("{}/nodes.parquet", dst)).unwrap();
    let w = ParquetWriter::new(f);
    w.finish(&mut df).unwrap();

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
    let mut df = df!("id" => &ids).unwrap();

    let w = ParquetWriter::new(std::fs::File::create(format!("{}/ways.parquet", dst)).unwrap());
    w.finish(&mut df).unwrap();

    //
    // store tags
    //

    println!(" >> tags.");
    tags2parquet(&data.ways, &format!("{}/way-tags.parquet", dst));

    //
    // store node refs
    //

    println!(" >> node refs.");
    let refs = data
        .ways
        .iter()
        .map(|w| w.nodes.iter().map(|n| n.r#ref).collect::<Vec<i64>>())
        .flatten()
        .collect::<Vec<i64>>();

    let ids = data
        .ways
        .iter()
        .map(|w| std::iter::repeat(w.id).take(w.nodes.len()))
        .flatten()
        .collect::<Vec<i64>>();

    let mut df = df!("id" => ids, "nodeid" => refs).unwrap();
    let w =
        ParquetWriter::new(std::fs::File::create(format!("{}/way-nodes.parquet", dst)).unwrap());
    w.finish(&mut df).unwrap();
}

fn rels2parquet(data: &osm::File, dst: &str) {
    //
    // store relations
    //

    let ids = data.relations.iter().map(|r| r.id).collect::<Vec<i64>>();
    let mut df = df!("id" => &ids).unwrap();

    let w =
        ParquetWriter::new(std::fs::File::create(format!("{}/relations.parquet", dst)).unwrap());
    w.finish(&mut df).unwrap();

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
                .map(|m| m.r#type.as_ref())
                .collect::<Vec<&str>>()
        })
        .flatten()
        .collect::<Vec<&str>>();

    let memrole = data
        .relations
        .iter()
        .map(|r| {
            r.members
                .iter()
                .map(|m| m.role.as_ref())
                .collect::<Vec<&str>>()
        })
        .flatten()
        .collect::<Vec<&str>>();

    let ids = data
        .relations
        .iter()
        .map(|e| std::iter::repeat(e.id).take(e.members.len()))
        .flatten()
        .collect::<Vec<i64>>();

    let mut df =
        df!("relation" => ids, "member" => memid, "type" => memtype, "role" => memrole).unwrap();
    let w = ParquetWriter::new(
        std::fs::File::create(format!("{}/relation-members.parquet", dst)).unwrap(),
    );
    w.finish(&mut df).unwrap();
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
