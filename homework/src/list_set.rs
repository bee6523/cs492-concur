#![allow(clippy::mutex_atomic)]
use std::cmp;
use std::ptr;
use std::sync::{Mutex, MutexGuard};

#[derive(Debug)]
struct Node<T> {
    data: T,
    next: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for Node<T> {}
unsafe impl<T> Sync for Node<T> {}

/// Concurrent sorted singly linked list using lock-coupling.
#[derive(Debug)]
pub struct OrderedListSet<T> {
    head: Mutex<*mut Node<T>>,
}

unsafe impl<T> Send for OrderedListSet<T> {}
unsafe impl<T> Sync for OrderedListSet<T> {}

// reference to the `next` field of previous node which points to the current node
struct Cursor<'l, T>(MutexGuard<'l, *mut Node<T>>);

impl<T> Node<T> {
    fn new(data: T, next: *mut Self) -> *mut Self {
        Box::into_raw(Box::new(Self {
            data,
            next: Mutex::new(next),
        }))
    }
}

impl<'l, T: Ord> Cursor<'l, T> {
    /// Move the cursor to the position of key in the sorted list. If the key is found in the list,
    /// return `true`.
    fn find(&mut self, key: &T) -> bool {
        unsafe{
            loop{
                let start = &self.0;
                let ptr = **start;
                if ptr.is_null(){
                    return false;
                }
                if &((*ptr).data) == key {
                    return true;
                }else if &((*ptr).data) > key{
                    return false;
                }
                
                self.0 = (*ptr).next.lock().unwrap();
            }
        }
    }
}

impl<T> OrderedListSet<T> {
    /// Creates a new list.
    pub fn new() -> Self {
        Self {
            head: Mutex::new(ptr::null_mut()),
        }
    }
}

impl<T: Ord> OrderedListSet<T> {
    fn find(&self, key: &T) -> (bool, Cursor<T>) {
        let mut cursor = Cursor(self.head.lock().unwrap());
        (cursor.find(key), cursor)
    }

    /// Returns `true` if the set contains the key.
    pub fn contains(&self, key: &T) -> bool {
        self.find(key).0
    }

    /// Insert a key to the set. If the set already has the key, return the provided key in `Err`.
    pub fn insert(&self, key: T) -> Result<(), T> {
        let (succ, mut cursor) = self.find(&key);
        if succ{
            Err(key)
        }else{
            *cursor.0 = Node::new(key,*cursor.0);
            Ok(())
        }
    }

    /// Remove the key from the set and return it.
    pub fn remove(&self, key: &T) -> Result<T, ()> {
        let (succ, mut cursor) = self.find(key);
        if succ {
            unsafe{
                let curnode = Box::from_raw(*cursor.0);
                let nextlock = curnode.next.lock().unwrap();
                *cursor.0 = *nextlock;
                Ok(curnode.data)
            }
        }else{
            Err(())
        }
    }
}

#[derive(Debug)]
pub struct Iter<'l, T>(Option<MutexGuard<'l, *mut Node<T>>>);

impl<T> OrderedListSet<T> {
    /// An iterator visiting all elements.
    pub fn iter(&self) -> Iter<T> {
        Iter(Some(self.head.lock().unwrap()))
    }
}

impl<'l, T> Iterator for Iter<'l, T> {
    type Item = &'l T;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.0{
            Some(guard) => {
                let ptr = **guard;
                if ptr.is_null(){
                    self.0 = None;
                    None
                }else{
                    unsafe{
                        let next = (*ptr).next.lock().unwrap();
                        let val = &((*ptr).data);
                        self.0 = Some(next);
                        Some(val)
                    }
                }
            }
            None => None
        }
    }
}

impl<T> Drop for OrderedListSet<T> {
    fn drop(&mut self) {
        let mut np = self.head.lock().unwrap();//node pointer
        while !((*np).is_null()) {
            unsafe{
                let node = Box::from_raw(*np);
                let next = node.next.lock().unwrap();
                *np = *next;
            }
        }
    }
}

impl<T> Default for OrderedListSet<T> {
    fn default() -> Self {
        Self::new()
    }
}
