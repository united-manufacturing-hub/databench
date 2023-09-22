use criterion::{criterion_group, criterion_main, Criterion};
use databench_rs::generator::chernobyl::Chernobyl;
use databench_rs::generator::Generator;

/*
fn generator_chernobyl_benchmark(c: &mut Criterion) {
    c.bench_function("Chernobyl new", |b| b.iter(|| Chernobyl::new(4, 10000)));
    c.bench_function("Chernobyl get messages", |b| {
        let chernobyl = Chernobyl::new(4, 10000).expect("Error creating Chernobyl");
        b.iter(|| chernobyl.get_message().expect("Error getting message"))
    });
}
*/

fn bench_crypto(c: &mut Criterion) {
    let chernobyl = Chernobyl::new(4, 10000).expect("Error creating Chernobyl");
    let messages = (0..1000)
        .map(|_| {
            let msg = chernobyl.get_message().expect("Error getting message");
            format!("{:?}", msg)
        })
        .collect::<Vec<String>>();

    c.bench_function("blake3", |b| {
        b.iter(|| {
            let mut hasher = blake3::Hasher::new();
            for msg in &messages {
                hasher.update(msg.as_bytes());
            }
            hasher.finalize()
        })
    });

    c.bench_function("sha3", |b| {
        b.iter(|| {
            use sha3::Digest;
            let mut hasher = sha3::Sha3_256::new();
            for msg in &messages {
                hasher.update(msg.as_bytes());
            }
            hasher.finalize()
        })
    });
}

criterion_group!(benches, bench_crypto);
criterion_main!(benches);
