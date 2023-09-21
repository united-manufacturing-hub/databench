use rand::{thread_rng, Rng};

pub fn rand_entry<T>(entries: &[T]) -> &T {
    &entries[thread_rng().gen_range(0..entries.len())]
}
