use crate::pq;
use osm::{
    proto::{items::PrimitiveBlock, FileBlock, FileBlockIterator},
    Node, Relation, Way,
};
use std::{
    fs::File,
    sync::{
        mpsc::{sync_channel, Receiver, SyncSender},
        Arc, Mutex,
    },
    thread,
};

struct Worker {
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(
        rx: Arc<Mutex<Receiver<PrimitiveBlock>>>,
        sender_node: SyncSender<Vec<Node>>,
        sender_of_ways: SyncSender<Vec<Way>>,
        sender_of_relations: SyncSender<Vec<Relation>>,
    ) -> Self {
        let th = thread::spawn(move || loop {
            let msg = rx.lock().unwrap().recv();

            let pb = match msg {
                Ok(pb) => pb,
                Err(_) => break,
            };

            let str_tbl = osm::proto::parse_str_tbl(&pb);

            // iterate over primitive groups
            for pg in &pb.primitivegroup {
                if let Some(dense) = &pg.dense {
                    sender_node
                        .send(
                            osm::Node::from_proto_dense_nodes(&dense, &str_tbl, &pb)
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
                                .map(|node| osm::Node::from_proto(&node, &str_tbl, &pb))
                                .collect::<Vec<osm::Node>>(),
                        )
                        .unwrap();
                }
            }
        });

        Worker { thread: Some(th) }
    }
}

struct ThreadPool {
    workers: Vec<Worker>,
    tx: Option<SyncSender<PrimitiveBlock>>,

    tx_nd: Option<SyncSender<Vec<Node>>>,
    tx_wy: Option<SyncSender<Vec<Way>>>,
    tx_rl: Option<SyncSender<Vec<Relation>>>,

    nodes: Option<thread::JoinHandle<()>>,
    ways: Option<thread::JoinHandle<()>>,
    relations: Option<thread::JoinHandle<()>>,
}

impl ThreadPool {
    fn new(n: usize, dst: &str) -> Self {
        let bufsize: usize = 1000000;
        let chansize: usize = 100000;

        // Nodes

        let fp_nodes = format!("{}/nodes.parquet", dst);
        let fp_node_tags = format!("{}/node-tags.parquet", dst);

        let (sender_node, receiver_node) = std::sync::mpsc::sync_channel(chansize);
        let worker_nodes = std::thread::spawn(move || {
            // writers
            let mut wnode = pq::writer(pq::SCHEMA_NODE, File::create(fp_nodes).unwrap()).unwrap();
            let mut wnode_tags =
                pq::writer(pq::SCHEMA_TAGS, File::create(fp_node_tags).unwrap()).unwrap();

            // internal buffer
            let mut buf = Vec::<osm::Node>::with_capacity(bufsize);

            // listen for work
            while let Ok(mut vs) = receiver_node.recv() {
                buf.append(&mut vs);

                if buf.len() >= bufsize {
                    pq::write_nodes(&buf, &mut wnode);
                    pq::write_tags(&buf, &mut wnode_tags);
                    buf.clear();
                }
            }

            // flush buffer
            pq::write_nodes(&buf, &mut wnode);
            pq::write_tags(&buf, &mut wnode_tags);

            // close writers
            wnode.close().unwrap();
            wnode_tags.close().unwrap();
        });

        // Ways

        let fp_ways = format!("{}/ways.parquet", dst);
        let fp_way_tags = format!("{}/way-tags.parquet", dst);
        let fp_way_nodes = format!("{}/way-nodes.parquet", dst);

        let (sender_of_ways, receiver_of_ways) = sync_channel(chansize);
        let worker_ways = std::thread::spawn(move || {
            // writers
            let mut wway = pq::writer(pq::SCHEMA_WAY, File::create(fp_ways).unwrap()).unwrap();
            let mut wway_tags =
                pq::writer(pq::SCHEMA_TAGS, File::create(fp_way_tags).unwrap()).unwrap();
            let mut wway_nodes =
                pq::writer(pq::SCHEMA_WAY_NODE, File::create(fp_way_nodes).unwrap()).unwrap();

            // buffer
            let mut buf = Vec::<osm::Way>::with_capacity(bufsize);

            // listen for work
            while let Ok(mut vs) = receiver_of_ways.recv() {
                buf.append(&mut vs);
                if buf.len() >= bufsize {
                    pq::write_ways(&buf, &mut wway);
                    pq::write_tags(&buf, &mut wway_tags);
                    pq::write_way_nodes(&buf, &mut wway_nodes);
                    buf.clear();
                }
            }

            // flush buffers
            pq::write_ways(&buf, &mut wway);
            pq::write_tags(&buf, &mut wway_tags);
            pq::write_way_nodes(&buf, &mut wway_nodes);

            // close writer
            wway.close().unwrap();
            wway_tags.close().unwrap();
            wway_nodes.close().unwrap();
        });

        // Relations

        let fp_rels = format!("{}/relations.parquet", dst);
        let fp_rel_tags = format!("{}/relation-tags.parquet", dst);
        let fp_rel_mems = format!("{}/relation-members.parquet", dst);

        let (sender_of_relations, receiver_of_relations) = std::sync::mpsc::sync_channel(chansize);
        let worker_relations = std::thread::spawn(move || {
            // writers
            let mut wrel = pq::writer(pq::SCHEMA_RELATION, File::create(fp_rels).unwrap()).unwrap();
            let mut wrel_tags =
                pq::writer(pq::SCHEMA_TAGS, File::create(fp_rel_tags).unwrap()).unwrap();
            let mut wrel_mem = pq::writer(
                pq::SCHEMA_RELATION_MEMBER,
                File::create(fp_rel_mems).unwrap(),
            )
            .unwrap();

            // buffer
            let mut buf = Vec::<osm::Relation>::with_capacity(bufsize);

            // listen for work
            while let Ok(mut vs) = receiver_of_relations.recv() {
                buf.append(&mut vs);
                if buf.len() >= bufsize {
                    pq::write_relations(&buf, &mut wrel);
                    pq::write_tags(&buf, &mut wrel_tags);
                    pq::write_relation_members(&buf, &mut wrel_mem);
                    buf.clear();
                }
            }

            // flush buffer
            pq::write_relations(&buf, &mut wrel);
            pq::write_tags(&buf, &mut wrel_tags);
            pq::write_relation_members(&buf, &mut wrel_mem);

            // close writers
            wrel.close().unwrap();
            wrel_tags.close().unwrap();
            wrel_mem.close().unwrap();
        });

        // Workers

        let (tx, rx) = sync_channel::<PrimitiveBlock>(500);

        let rx = Arc::new(Mutex::new(rx));
        let mut ws = Vec::<Worker>::with_capacity(n);
        for _ in 0..n {
            ws.push(Worker::new(
                Arc::clone(&rx),
                sender_node.clone(),
                sender_of_ways.clone(),
                sender_of_relations.clone(),
            ));
        }

        ThreadPool {
            workers: ws,
            tx: Some(tx),
            tx_nd: Some(sender_node),
            tx_wy: Some(sender_of_ways),
            tx_rl: Some(sender_of_relations),
            nodes: Some(worker_nodes),
            ways: Some(worker_ways),
            relations: Some(worker_relations),
        }
    }

    fn send(&self, p: PrimitiveBlock) {
        self.tx.as_ref().unwrap().send(p).unwrap();
    }
}

impl Drop for ThreadPool {
    fn drop(&mut self) {
        println!("JOIN!");

        drop(self.tx.take());

        for w in &mut self.workers {
            w.thread.take().unwrap().join().unwrap();
        }

        drop(self.tx_nd.take());
        drop(self.tx_wy.take());
        drop(self.tx_rl.take());

        self.nodes.take().unwrap().join().unwrap();
        self.ways.take().unwrap().join().unwrap();
        self.relations.take().unwrap().join().unwrap();
    }
}

pub fn export(r: impl std::io::Read + 'static, dst: &str, _: Option<usize>, _: Option<usize>) {
    let pool = ThreadPool::new(8, dst);

    for fb in FileBlockIterator::from_reader(r) {
        if let FileBlock::Primitive(b) = fb {
            pool.send(b);
        }
    }
}
