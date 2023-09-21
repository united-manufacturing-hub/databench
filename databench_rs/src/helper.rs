use rand::{Rng, thread_rng};
use rand::rngs::ThreadRng;

pub fn rand_entry<T>(entries: &[T]) -> &T {
    &entries[thread_rng().gen_range(0..entries.len())]
}
