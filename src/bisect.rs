

use std::cmp::PartialOrd;
use std::fmt::Display;
use std::fmt::Debug;


pub fn bisect_right<D>(sorted_key: &Vec<D>, search_key: D, lo: Option<usize>,  hi: Option<usize>) -> usize
    where D: PartialOrd + Display + Debug {
    ///Return the index where to insert item x in list a, assuming a is sorted.
    ///
    ///The return value i is such that all e in a[:i] have e <= x, and all e in
    ///a[i:] have e > x.  So if x already appears in the list, a.insert(x) will
    ///insert just after the rightmost x already there.
    ///
    ///Optional args lo (default 0) and hi (default len(a)) bound the
    ///slice of a to be searched.
    let mut hi = match hi {
        Some(num) => num,
        None => sorted_key.len()
    };

    let mut lo = match lo {
        Some(num) => num,
        None => 0
    };
    while lo < hi {
        let mid = (lo + hi) / 2 ;
        if search_key < sorted_key[mid] {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }

    lo
}
