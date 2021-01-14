//! This library provides a [`Ring`] struct which allows for reading and writing
//! of a ring-buffer/capped-queue of elements with multiple readers and multiple
//! writers concurrently.
//!
//! A blog article recounting the development process and overall design can be
//! found at [kmdreko.github.io](https://kmdreko.github.io/posts/20191003/a-simple-lock-free-ring-buffer/).

use std::alloc::{alloc, dealloc, Layout};
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};

/// This structure provides access to elements in a ring buffer from multiple
/// concurrent readers and writers in a lock-free manner.
///
/// The class works by allocating the array and storing two indexes (for the
/// beginning and end of the allocated space). Two atomic indexes are used to
/// track the beginning and end of the currently used storage space. To
/// facilitate concurrent reads and writes, theres a read start index before
/// the read end index for data currently being read, and a corresponding write
/// start index before the write end index for data currently being written.
/// These start indexes cannot overlap. Just using these indexes suffer from
/// some minute inefficiencies and a few ABA problems. Therfore, atomic
/// integers are used to store the currently used and currently free sizes.
///
/// It allows multiple readers and multiple writers by implementing a reserve-
/// commit system. A thread wanting to read will check the used size to see if
/// there's enough data. If there is, it subtracts from the used size to
/// 'reserve' the read. It then does a compare-exchange to 'commit' by
/// increasing the read end index. If that fails, then it backs out ('un-
/// reserves') by adding back to the used size and tries again. If it
/// succeeds, then it proceeds to read the data. In order to complete, the
/// reader must update the read start index to where it just finished
/// reading from. However, because other readers that started before may not be
/// done yet, the reader must wait until the read start index points to
/// where the read started. Only, then is the read start index updated, and
/// the free size increased. So while this implementation is lock-free, it is
/// not wait-free. This same principle works the same when writing (ammended
/// for the appropriate indexes).
///
/// If two readers try to read at the same time and there is only enough data
/// for one of them. The used size MAY be negative because they both 'reserve'
/// the data. This is an over-reserved state. But the compare-exchange will
/// only allow one reader to 'commit' to the read and the other will 'un-
/// reserve' the read.
///
/// ```text
///   |data          |r_end     used=5             |w_end     - free
///   |----|----|++++|====|====|====|====|====|++++|----|     + modifying
///    free=3   |r_start                      |w_start  |     = used
/// ```
///
/// The diagram above shows a ring of 10 capactiy storing 5 elements with a
/// reader reading an element while a writer is writing another.
#[derive(Debug)]
pub struct Ring<T> {
    raw_data: NonNull<T>,
    raw_size: usize,
    used_size: AtomicIsize,
    free_size: AtomicIsize,
    read_start: AtomicUsize,
    read_end: AtomicUsize,
    write_start: AtomicUsize,
    write_end: AtomicUsize,

    _marker: PhantomData<T>,
}

impl<T> Ring<T> {
    /// Creates a new `Ring` with the specified capacity.
    ///
    /// # Panics
    ///
    /// This function will panic if `size_of::<T>() * capacity` overflows or on
    /// out-of-memory error.
    pub fn new(capacity: usize) -> Self {
        let raw_data = if std::mem::size_of::<T>() != 0 {
            let layout = Layout::array::<T>(capacity).unwrap();
            let data = unsafe { alloc(layout) };
            NonNull::new(data as *mut T).unwrap()
        } else {
            NonNull::dangling()
        };

        Ring {
            raw_data,
            raw_size: capacity,
            used_size: AtomicIsize::new(0),
            free_size: AtomicIsize::new(capacity as isize),
            read_start: AtomicUsize::new(0),
            read_end: AtomicUsize::new(0),
            write_start: AtomicUsize::new(0),
            write_end: AtomicUsize::new(0),

            _marker: PhantomData,
        }
    }

    /// Returns the number of elements not currently being read or written.
    pub fn len(&self) -> usize {
        // the `used_size` can be negative if the ring is over-reserved, for
        // simplicity this returns 0 in that case
        self.used_size.load(Ordering::Acquire).max(0) as usize
    }

    /// Returns the total number of elements the ring can hold.
    pub fn capacity(&self) -> usize {
        self.raw_size
    }

    /// Sends an element into the ring.
    ///
    /// This will block until it succeeds, meaning no progress will be made if
    /// the ring is full or if writers readers continually win. Use
    /// [`try_send`](Ring::try_send) to handle no-space-to-write cases.
    pub fn send(&self, value: T) {
        let offset = spin(|| self.acquire_write_space());
        unsafe { self.raw_data.as_ptr().offset(offset).write(value) };
        spin(|| self.release_write_space(offset));
    }

    /// Sends an element into the ring.
    ///
    /// This will return an error and not block if there is no space in the ring
    /// to write to.
    pub fn try_send(&self, value: T) -> Result<(), SendError<T>> {
        let offset = match self.acquire_write_space() {
            Some(offset) => offset,
            None => return Err(SendError(value)),
        };

        unsafe { self.raw_data.as_ptr().offset(offset).write(value) };
        spin(|| self.release_write_space(offset));
        Ok(())
    }

    /// Receives an element from the ring.
    ///
    /// This will block until it succeeds, meaning no progress will be made if
    /// no elements are being written or if other readers continually win. Use
    /// [`try_recv`](Ring::try_recv) to handle nothing-to-read cases.
    pub fn recv(&self) -> T {
        let offset = spin(|| self.acquire_read_space());
        let value = unsafe { self.raw_data.as_ptr().offset(offset).read() };
        spin(|| self.release_read_space(offset));
        value
    }

    /// Receives an element from the ring.
    ///
    /// This will return an error and not block if there are no elements in the
    /// ring.
    pub fn try_recv(&self) -> Result<T, RecvError> {
        let offset = match self.acquire_read_space() {
            Some(offset) => offset,
            None => return Err(RecvError),
        };

        let value = unsafe { self.raw_data.as_ptr().offset(offset).read() };
        spin(|| self.release_read_space(offset));
        Ok(value)
    }

    #[rustfmt::skip]
    fn acquire_read_space(&self) -> Option<isize> {
        loop {
            // determine the new read end
            let old_read_end = self.read_end.load(Ordering::Acquire);
            let new_read_end = (old_read_end + 1) % self.raw_size;

            // try to reserve space, if not enough immediately un-reserve the
            // space and return failure
            if self.used_size.fetch_sub(1, Ordering::SeqCst) < 1 {
                self.used_size.fetch_add(1, Ordering::AcqRel);
                return None;
            }

            // try to commit the read
            if self.read_end.compare_and_swap(old_read_end, new_read_end, Ordering::SeqCst) == old_read_end {
                return Some(old_read_end as isize);
            }

            // if failed to commit, un-reserve the space and try again
            self.used_size.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn release_read_space(&self, offset: isize) -> Option<()> {
        let old_read_start = offset as usize;
        let new_read_start = (old_read_start + 1) % self.raw_size;
        if self.read_start.load(Ordering::Acquire) != old_read_start {
            return None;
        }

        self.read_start.store(new_read_start, Ordering::SeqCst);
        self.free_size.fetch_add(1, Ordering::AcqRel);
        Some(())
    }

    #[rustfmt::skip]
    fn acquire_write_space(&self) -> Option<isize> {
        loop {
            // determine the new write end
            let old_write_end = self.write_end.load(Ordering::Acquire);
            let new_write_end = (old_write_end + 1) % self.raw_size;

            // try to reserve space, if not enough immediately un-reserve the
            // space and return failure
            if self.free_size.fetch_sub(1, Ordering::SeqCst) < 1 {
                self.free_size.fetch_add(1, Ordering::AcqRel);
                return None;
            }

            // try to commit the write
            if self.write_end.compare_and_swap(old_write_end, new_write_end, Ordering::SeqCst) == old_write_end {
                return Some(old_write_end as isize);
            }

            // if failed to commit, un-reserve the space and try again
            self.free_size.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn release_write_space(&self, offset: isize) -> Option<()> {
        let old_write_start = offset as usize;
        let new_write_start = (old_write_start + 1) % self.raw_size;
        if self.write_start.load(Ordering::Acquire) != old_write_start {
            return None;
        }

        self.write_start.store(new_write_start, Ordering::SeqCst);
        self.used_size.fetch_add(1, Ordering::AcqRel);
        Some(())
    }
}

unsafe impl<T: Send> Send for Ring<T> {}
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T> Drop for Ring<T> {
    fn drop(&mut self) {
        let start = self.read_end.load(Ordering::SeqCst);
        let end = self.write_start.load(Ordering::SeqCst);

        let mut current = start;
        while current != end {
            let data = unsafe { self.raw_data.as_ptr().offset(current as isize).read() };
            std::mem::drop(data);
            current = (current + 1) % self.raw_size;
        }

        if std::mem::size_of::<T>() != 0 {
            let layout = Layout::array::<T>(self.raw_size).unwrap();
            let data = self.raw_data.as_ptr();
            unsafe { dealloc(data as *mut u8, layout) };
        }
    }
}

/// An error returned by [`Ring::try_send`] if there is no space in the ring to
/// write to. It contains the element that was to be sent.
pub struct SendError<T>(pub T);

/// An error returned by [`Ring::try_recv`] if there are no elements in the ring.
pub struct RecvError;

fn spin<T, F: FnMut() -> Option<T>>(mut f: F) -> T {
    let mut i: usize = 1;
    loop {
        match f() {
            Some(value) => return value,
            None => {
                i = i.wrapping_add(1);
                if i % 16 == 0 {
                    std::thread::yield_now()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Ring;
    use std::sync::atomic::{AtomicI32, Ordering};
    use std::sync::Arc;

    #[test]
    fn can_send_and_recv_single_value() {
        let ring = Ring::<i32>::new(10);
        ring.send(5);
        assert_eq!(ring.recv(), 5);
    }

    #[test]
    fn can_send_and_recv_multiple_values() {
        let ring = Ring::<i32>::new(10);
        ring.send(1);
        ring.send(2);
        ring.send(3);
        ring.send(4);
        ring.send(5);
        assert_eq!(ring.recv(), 1);
        assert_eq!(ring.recv(), 2);
        assert_eq!(ring.recv(), 3);
        assert_eq!(ring.recv(), 4);
        assert_eq!(ring.recv(), 5);
    }

    #[test]
    fn can_send_and_recv_from_different_threads() {
        let ring = Arc::new(Ring::<i32>::new(10));
        let total = Arc::new(AtomicI32::new(0));

        let write_thread = {
            let ring = Arc::clone(&ring);
            std::thread::spawn(move || {
                for i in 0..100 {
                    ring.send(i);
                }
            })
        };

        let read_thread = {
            let ring = Arc::clone(&ring);
            let total = Arc::clone(&total);
            std::thread::spawn(move || {
                for _ in 0..100 {
                    let i = ring.recv();
                    total.fetch_add(i, Ordering::SeqCst);
                }
            })
        };

        write_thread.join().unwrap();
        read_thread.join().unwrap();

        assert_eq!(total.load(Ordering::SeqCst), 4950);
    }
}
