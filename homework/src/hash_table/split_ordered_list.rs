//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{unprotected, Guard, Shared, Owned};
use lockfree::list::{Cursor, List, Node};

use super::growable_array::GrowableArray;
use crate::map::NonblockingMap;

/// Lock-free map from `usize` in range [0, 2^63-1] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> {
    /// Lock-free list sorted by recursive-split order. Use `None` sentinel node value.
    list: List<usize, Option<V>>,
    /// array of pointers to the buckets
    buckets: GrowableArray<Node<usize, Option<V>>>,
    /// number of buckets
    size: AtomicUsize,
    /// number of items
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        let new_list = List::new();
        let new_buckets=GrowableArray::new();
        unsafe{
            new_list.harris_insert(0,None,unprotected());
            new_buckets.get(0,unprotected()).store(new_list.head(unprotected()).curr(),Ordering::Relaxed);
        }
        Self {
            list: new_list,
            buckets: new_buckets,
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }
}

impl<V> SplitOrderedList<V> {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(&'s self, index: usize, guard: &'s Guard) -> Cursor<'s, usize, Option<V>> {
        let bucket=self.buckets.get(index,guard);
        let node=bucket.load(Ordering::Acquire, guard);
        if node.is_null() {
            self.initialize_bucket(index,guard)
        }else{
            unsafe{ Cursor::from_raw(bucket, node.as_raw()) }
        }
    }
    fn initialize_bucket<'s>(&'s self, index: usize, guard: &'s Guard)->Cursor<'s, usize, Option<V>> {
        let parent_idx=self.get_parent(index);
        loop{
            let parent=self.buckets.get(parent_idx,guard);
            let parent_node=parent.load(Ordering::Acquire,guard);
            let mut cursor;
            if parent_node.is_null(){
                cursor=self.initialize_bucket(parent_idx,guard);
            }else{
                cursor= unsafe{ Cursor::from_raw(parent, parent_node.as_raw()) };
            }
        
            let key=self.sentinel_key(&index);
            if cursor.find_harris(&key,guard).unwrap() {
                return cursor;
            }else{
                let bucket=Owned::new(Node::new(key,None));
                match cursor.insert(bucket,guard){
                    Ok(_) => {
                        self.buckets.get(index,guard).store(cursor.curr(),Ordering::Release);
                    },
                    Err(e) => {
                        drop(e);
                        continue;
                    },
                }
            }
        }
    }
    fn get_parent(&self,index: usize)->usize{
        let mut parent=self.size.load(Ordering::Acquire);
        loop{
            parent = parent>>1;
            if parent <= index{
                break;
            }
        }
        return index-parent;
    }
    fn sentinel_key(&self, index: &usize)->usize{
        index.reverse_bits()
    }
    fn ord_key(&self, index: &usize)->usize{
        index.reverse_bits() | 1
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        let size = self.size.load(Ordering::Acquire);
        let index= key % size;
        loop{
            let mut cursor=self.lookup_bucket(index,guard);
            match cursor.find_harris(&(self.ord_key(key)), guard){
                Ok(found) => return (size,found,cursor),
                Err(_) => continue,
            }
        }
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> NonblockingMap<usize, V> for SplitOrderedList<V> {
    fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key,guard);

        if found {
            cursor.lookup().unwrap().as_ref()
        }else{
            None
        }
    }

    fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);
        let (size, found, mut cursor) = self.find(key,guard);

        if found{
            Err(value)
        }else{
            let node = Owned::new(Node::new(self.ord_key(key),Some(value)));
            match cursor.insert(node,guard){
                Ok(_) => {
                    let count=self.count.fetch_add(1,Ordering::Relaxed);
                    if count > size* Self::LOAD_FACTOR {
                        self.size.compare_and_swap(size,size<<1,Ordering::Relaxed);
                    }
                    Ok(())
                },
                Err(e) => Err((*(e.into_box())).into_value().unwrap()),
            }
        }
    }

    fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);
        let (_, found, cursor) = self.find(key,guard);

        if found{
            let ret=cursor.delete(guard).map(|n| n.as_ref().unwrap());
            if ret.is_ok(){
                self.count.fetch_sub(1,Ordering::Relaxed);
            }
            ret
        }else{
            Err(())
        }
    }
}
