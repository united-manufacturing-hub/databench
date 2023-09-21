use criterion::{criterion_group, criterion_main, Criterion};
use databench_rs::generator::chernobyl::Chernobyl;
use databench_rs::generator::Generator;

fn generator_chernobyl_benchmark(c: &mut Criterion) {
    c.bench_function("Chernobyl new", |b| {
        b.iter(|| {
            let chernobyl = Chernobyl::new(4,10000);
            chernobyl  // Return to prevent optimization removal
        })
    });
    c.bench_function("Chernobyl get messages", |b| {
        let chernobyl = Chernobyl::new(4,10000).expect("Error creating Chernobyl");
        b.iter(|| {
            let msg = chernobyl.get_message().expect("Error getting message");
            msg
        })
    });
}

criterion_group!(benches, generator_chernobyl_benchmark);
criterion_main!(benches);
