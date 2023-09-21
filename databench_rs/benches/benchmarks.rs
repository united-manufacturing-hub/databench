use criterion::{criterion_group, criterion_main, Criterion};
use databench_rs::generator::chernobyl::Chernobyl;
use databench_rs::generator::Generator;

fn generator_chernobyl_benchmark(c: &mut Criterion) {
    c.bench_function("Chernobyl new", |b| b.iter(|| Chernobyl::new(4, 10000)));
    c.bench_function("Chernobyl get messages", |b| {
        let chernobyl = Chernobyl::new(4, 10000).expect("Error creating Chernobyl");
        b.iter(|| chernobyl.get_message().expect("Error getting message"))
    });
}

criterion_group!(benches, generator_chernobyl_benchmark);
criterion_main!(benches);
