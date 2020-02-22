use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;
use md5;
use bisect::bisect_right;


pub struct ConsistentHashing<T: ToString + Hash + Clone + WithWeightInfo> {
    hashing_ring: HashMap<u32, T>,
    real_nodes: HashMap<String, T>,
    sorted_keys: Vec<u32>,
    interleave_count: usize,
    total_weight: usize,
}

impl<T: ToString + Hash + Clone + WithWeightInfo> ConsistentHashing<T> {
    pub fn new(real_nodes: &Vec<T>, interleave_count_setting: Option<usize>) -> ConsistentHashing<T> {
        ///when you are running a cluster of Memcached
        ///servers it could happen to not all server can allocate the
        ///same amount of memory. You might have a Memcached server
        ///with 128mb, 512mb, 128mb. If you would the array structure
        ///all servers would have the same weight in the consistent
        ///hashing scheme. Spreading the keys 33/33/33 over the servers.
        ///But as server 2 has more memory available you might want to
        ///give it more weight so more keys get stored on that server.
        ///When you are using a object, the key should represent the
        ///server location syntax and the value the weight of the server.
        ///

        let interleave_count = match interleave_count_setting {
            Some(count) => count,
            None => 40, //default value = 40
        };
        let mut new_consitent_hashing = ConsistentHashing {
            hashing_ring: HashMap::new(),
            real_nodes: HashMap::new(),
            sorted_keys: Vec::new(),
            interleave_count,
            total_weight: 0,
        };

        new_consitent_hashing.generate_hashing_ring(real_nodes);
        return new_consitent_hashing;
    }

    fn generate_hashing_ring(&mut self, real_nodes: &Vec<T>) {
        ///Generates the ring.
        ///
        //real nodes number
        let nodes_num = real_nodes.len();
        //calculate total weight
        let mut total_weight: usize = 0;
        for i in 0..nodes_num {
            total_weight += real_nodes[i].get_weight();
        }
        self.total_weight = total_weight;

        for i in 0..nodes_num {
            let node_entity = &real_nodes[i];
            //save real node
            self.real_nodes.insert(node_entity.to_string(), node_entity.clone());

            let weight = 0;
            let factor = ((self.interleave_count * nodes_num * weight) / total_weight) as usize;
            for j in 0..factor {
                let b_key = hash_digest(&format!("{}-{}", node_entity.to_string(), j));
                for i in 0..3 {
                    let key = hash_val(&b_key, Box::new(move |x| x+i*4));
                    self.hashing_ring.insert(key, node_entity.clone());
                    self.sorted_keys.push(key);
                }
            }
        }
        self.sorted_keys.sort();

    }


    fn get_node(&self, string_key: &String) -> Option<T>{
        ///Given a string key a corresponding node in the hash ring is returned.
        ///If the hash ring is empty, `None` is returned.

        let pos = self.get_node_pos(string_key);
        match pos {
            Some(pos) => self.hashing_ring[self.sorted_keys[pos]],
            None => None,
        }
    }

    fn get_node_pos(&self, string_key: &String) -> Option<T>{
        ///Given a string key a corresponding node in the hash ring is returned along with it's position in the ring.
        ///If the hash ring is empty, (`None`, `None`) is returned.

        if self.hashing_ring.len() <= 0 {
            return None;
        }

        let key = gen_key(string_key);
        //https://rust-algo.club/searching/binary_search/index.html
        let mut pos = bisect_right(&self.sorted_keys, key, None, None);

        if pos == self.sorted_keys.len() {
            return 0;
        }else{
            return pos;
        }
    }



}


fn hashing<DT: Hash>(data: &DT) -> u64 {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

fn hash_digest(key: &String) -> Vec<u8> {
    let digest = md5::compute(key);
    digest.to_vec()
}

fn hash_val(b_key: &Vec<u8>, entry_fn: Box<dyn Fn(usize) -> usize>) -> u32 {
    (b_key[entry_fn(3)] as u32) << 24
        | (b_key[entry_fn(2)] as u32) << 16
        | (b_key[entry_fn(1)] as u32) << 8
        | (b_key[entry_fn(0)] as u32)
}


fn gen_key(string_key: &String) -> u32 {
    let b_key = hash_digest(string_key);
    hash_val(&b_key, Box::new(move |x| x))
}


pub trait WithWeightInfo {
    fn get_weight(&self) -> usize;
}

#[derive(Clone, Debug)]
pub struct NodeInfoWithWeigth {
    pub node_name: &'static str,
    pub weight: usize,
}

impl ToString for NodeInfoWithWeigth {
    fn to_string(&self) -> String {
        format!("{}", self.node_name)
    }
}

impl Hash for NodeInfoWithWeigth {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_name.hash(state);
    }
}

impl WithWeightInfo for NodeInfoWithWeigth {
    fn get_weight(&self) -> usize {
        self.weight
    }
}


#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub node_name: &'static str,
}

impl ToString for NodeInfo {
    fn to_string(&self) -> String {
        format!("{}", self.node_name)
    }
}

impl Hash for NodeInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.node_name.hash(state);
    }
}

impl WithWeightInfo for NodeInfo {
    fn get_weight(&self) -> usize {
        1
    }
}



#[cfg(test)]
mod tests {
    // 注意这个惯用法：在 tests 模块中，从外部作用域导入所有名字。
    use super::*;
    use crate::bisect::bisect_right;

    #[test]
    fn test_init() {
        let mut nodes: Vec<NodeInfo>= Vec::new();
        nodes.push(NodeInfo{node_name: "192.168.0.101:11212"});
        let consistent_hasing_ring = ConsistentHashing::new(&nodes, Some(40));
        //assert_eq!(add(1, 2), 3);
    }


    #[test]
    fn test_besect() {
        let vec: Vec<usize> = vec!(1,2,3,4,5);
        assert_eq!(bisect_right(&vec, 6, None, None), 5);
        //assert_eq!(add(1, 2), 3);
    }

}
