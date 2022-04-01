#![feature(test)]

extern crate test;

use {
    blstrs::Scalar,
    kzg::{
        coeff_form::KZGProver, coeff_form::KZGVerifier, polynomial::Polynomial, setup, KZGParams,
        KZGWitness,
    },
    lazy_static::lazy_static,
    rayon::prelude::*,
    rayon::ThreadPool,
    rs_merkle::{MerkleProof, MerkleTree},
    sha2::{Digest, Sha256},
    solana_kzg::kzg::{hash_packets, test_create_random_packet_data, test_create_trusted_setup},
    //solana_merkle_tree::MerkleTree,
    solana_program::hash::{hash, hashv, Hash, Hasher},
    solana_rayon_threadlimit::get_thread_count,
    std::time::Instant,
    test::Bencher,
};

const FEC_SET_SIZE: usize = 96;

lazy_static! {
    static ref PAR_THREAD_POOL: ThreadPool = rayon::ThreadPoolBuilder::new()
        //.num_threads(get_thread_count())
        .num_threads(8)
        .thread_name(|ix| format!("kzg_{}", ix))
        .build()
        .unwrap();
}

fn create_fec_set_packets(count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|_| test_create_random_packet_data())
        .collect()
}

fn fec_set_to_xy_points(packets: &Vec<Vec<u8>>) -> (Vec<Scalar>, Vec<Scalar>) {
    let hashes = hash_packets(packets);
    let mut xs = Vec::default();
    for i in 0..hashes.len() {
        xs.push(Scalar::from_u64s_le(&[0, 0, 0, i as u64]).unwrap());
    }
    let ys: Vec<_> = hashes
        .iter()
        .map(|h| Scalar::from_bytes_le(&h).unwrap())
        .collect();
    (xs, ys)
}

fn create_test_setup(fec_set_size: usize) -> (KZGParams, Vec<Scalar>, Vec<Scalar>) {
    let fec_set = create_fec_set_packets(fec_set_size);
    let (xs, ys) = fec_set_to_xy_points(&fec_set);
    let params = test_create_trusted_setup(fec_set_size);
    (params, xs, ys)
}

#[bench]
fn bench_create_interpolation(b: &mut Bencher) {
    let (params, xs, ys) = create_test_setup(FEC_SET_SIZE);
    b.iter(|| {
        let _interpolation = Polynomial::lagrange_interpolation(&xs, &ys);
    });
}

#[bench]
fn bench_create_commitment(b: &mut Bencher) {
    let (params, xs, ys) = create_test_setup(FEC_SET_SIZE);
    let interpolation = Polynomial::lagrange_interpolation(&xs, &ys);
    b.iter(|| {
        let prover = KZGProver::new(&params);
        let _commitment = prover.commit(&interpolation);
    });
}

#[bench]
fn bench_create_witness(b: &mut Bencher) {
    let (params, xs, ys) = create_test_setup(FEC_SET_SIZE);
    let prover = KZGProver::new(&params);
    let interpolation = Polynomial::lagrange_interpolation(&xs, &ys);
    b.iter(|| {
        prover
            .create_witness(&interpolation, (xs[0], ys[0]))
            .unwrap();
    });
}

#[bench]
fn bench_create_fec_set_witnesses(b: &mut Bencher) {
    let (params, xs, ys) = create_test_setup(FEC_SET_SIZE);
    let prover = KZGProver::new(&params);
    let interpolation = Polynomial::lagrange_interpolation(&xs, &ys);
    b.iter(|| {
        let _witnesses: Vec<_> = xs
            .iter()
            .zip(ys.iter())
            .map(|(x, y)| prover.create_witness(&interpolation, (*x, *y)).unwrap())
            .collect();
    });
}

#[bench]
fn bench_create_fec_set_witnesses_rayon(b: &mut Bencher) {
    let (params, xs, ys) = create_test_setup(FEC_SET_SIZE);
    let prover = KZGProver::new(&params);
    let interpolation = Polynomial::lagrange_interpolation(&xs, &ys);
    let points: Vec<_> = xs.into_iter().zip(ys.into_iter()).collect();

    b.iter(|| {
        PAR_THREAD_POOL.install(|| {
            let _witnesses: Vec<KZGWitness> = points
                .par_iter()
                .map(|(x, y)| prover.create_witness(&interpolation, (*x, *y)).unwrap())
                .collect();
        });
    });
}

#[bench]
#[ignore]
fn bench_create_fec_set_witnesses_rayon_chunked(b: &mut Bencher) {
    let (params, xs, ys) = create_test_setup(FEC_SET_SIZE);
    let prover = KZGProver::new(&params);
    let interpolation = Polynomial::lagrange_interpolation(&xs, &ys);
    let points: Vec<_> = xs.into_iter().zip(ys.into_iter()).collect();

    b.iter(|| {
        PAR_THREAD_POOL.install(|| {
            let _witnesses: Vec<KZGWitness> = points
                .par_chunks(6)
                .flat_map(|points| {
                    points
                        .iter()
                        .map(|(x, y)| prover.create_witness(&interpolation, (*x, *y)).unwrap())
                        .collect::<Vec<_>>()
                })
                .collect();
        });
    });
}

#[bench]
fn bench_verify_witness(b: &mut Bencher) {
    let (params, xs, ys) = create_test_setup(FEC_SET_SIZE);
    let prover = KZGProver::new(&params);
    let interpolation = Polynomial::lagrange_interpolation(&xs, &ys);
    let commitment = prover.commit(&interpolation);
    let witnesses: Vec<_> = xs
        .iter()
        .zip(ys.iter())
        .map(|(x, y)| prover.create_witness(&interpolation, (*x, *y)).unwrap())
        .collect();
    let verifier = KZGVerifier::new(&params);
    b.iter(|| {
        assert!(verifier.verify_eval((xs[0], ys[0]), &commitment, &witnesses[0]));
    });
}

#[bench]
#[ignore]
fn bench_verify_fec_set_witnesses(b: &mut Bencher) {
    let (params, xs, ys) = create_test_setup(FEC_SET_SIZE);
    let prover = KZGProver::new(&params);
    let interpolation = Polynomial::lagrange_interpolation(&xs, &ys);
    let commitment = prover.commit(&interpolation);
    let witnesses: Vec<_> = xs
        .iter()
        .zip(ys.iter())
        .map(|(x, y)| prover.create_witness(&interpolation, (*x, *y)).unwrap())
        .collect();
    b.iter(|| {
        let verifier = KZGVerifier::new(&params);
        for i in 0..witnesses.len() {
            assert!(verifier.verify_eval((xs[i], ys[i]), &commitment, &witnesses[i]));
        }
    });
}

#[bench]
fn bench_verify_fec_set_witnesses_rayon(b: &mut Bencher) {
    let (params, xs, ys) = create_test_setup(FEC_SET_SIZE);
    let prover = KZGProver::new(&params);
    let interpolation = Polynomial::lagrange_interpolation(&xs, &ys);
    let commitment = prover.commit(&interpolation);
    let witnesses: Vec<_> = xs
        .iter()
        .zip(ys.iter())
        .map(|(x, y)| prover.create_witness(&interpolation, (*x, *y)).unwrap())
        .collect();
    b.iter(|| {
        let verifier = KZGVerifier::new(&params);
        PAR_THREAD_POOL.install(|| {
            (0..witnesses.len()).into_par_iter().for_each(|i| {
                assert!(verifier.verify_eval((xs[i], ys[i]), &commitment, &witnesses[i]));
            });
        });
    });
}

/*
pub fn merkle_hash_packets(packets: &Vec<Vec<u8>>) -> Vec<Hash> {
    packets.iter().map(|p| hash(&p)).collect()
}
*/

#[bench]
fn bench_rs_merkle_create_tree(b: &mut Bencher) {
    let packets: Vec<_> = (0..FEC_SET_SIZE)
        .map(|_| test_create_random_packet_data())
        .collect();

    b.iter(|| {
        let leaves: Vec<[u8; 32]> = packets
            .iter()
            .map(|x| <[u8; 32]>::try_from(Sha256::digest(x).as_slice()).unwrap())
            .collect();
        let _merkle_tree = MerkleTree::<rs_merkle::algorithms::Sha256>::from_leaves(&leaves);
    });
}

#[bench]
fn bench_rs_merkle_create_proofs(b: &mut Bencher) {
    let packets: Vec<_> = (0..FEC_SET_SIZE)
        .map(|_| test_create_random_packet_data())
        .collect();

    let leaves: Vec<[u8; 32]> = packets
        .iter()
        .map(|x| <[u8; 32]>::try_from(Sha256::digest(x).as_slice()).unwrap())
        .collect();
    let merkle_tree = MerkleTree::<rs_merkle::algorithms::Sha256>::from_leaves(&leaves);

    b.iter(|| {
        let proofs: Vec<_> = (0..leaves.len()).map(|i| merkle_tree.proof(&[i])).collect();
    });
}

#[bench]
fn bench_rs_merkle_verify_proofs(b: &mut Bencher) {
    let packets: Vec<_> = (0..FEC_SET_SIZE)
        .map(|_| test_create_random_packet_data())
        .collect();

    let leaves: Vec<[u8; 32]> = packets
        .iter()
        .map(|x| <[u8; 32]>::try_from(Sha256::digest(x).as_slice()).unwrap())
        .collect();
    let merkle_tree = MerkleTree::<rs_merkle::algorithms::Sha256>::from_leaves(&leaves);

    let proofs: Vec<_> = (0..leaves.len()).map(|i| merkle_tree.proof(&[i])).collect();

    b.iter(|| {
        let merkle_root = merkle_tree.root().unwrap();
        for i in 0..leaves.len() {
            assert!(proofs[i].verify(merkle_root, &[i], &[leaves[i]], leaves.len()));
        }
    });
}

#[bench]
fn bench_solana_merkle_create_tree(b: &mut Bencher) {
    let packets: Vec<_> = (0..FEC_SET_SIZE)
        .map(|_| test_create_random_packet_data())
        .collect();

    //let hashes: Vec<Hash> = merkle_hash_packets(&packets);

    b.iter(|| {
        let _merkle_tree = solana_merkle_tree::MerkleTree::new(&packets);
    });
}

#[bench]
fn bench_solana_merkle_create_proofs(b: &mut Bencher) {
    let packets: Vec<_> = (0..FEC_SET_SIZE)
        .map(|_| test_create_random_packet_data())
        .collect();

    //let hashes: Vec<Hash> = merkle_hash_packets(&packets);

    let merkle_tree = solana_merkle_tree::MerkleTree::new(&packets);

    b.iter(|| {
        let _path_proofs: Vec<_> = (0..packets.len())
            .map(|i| merkle_tree.find_path(i).unwrap())
            .collect();
    });
}

const LEAF_PREFIX: &[u8] = &[0];
const INTERMEDIATE_PREFIX: &[u8] = &[1];

#[bench]
fn bench_solana_merkle_verify_proofs(b: &mut Bencher) {
    let packets: Vec<_> = (0..FEC_SET_SIZE)
        .map(|_| test_create_random_packet_data())
        .collect();

    //let hashes: Vec<Hash> = merkle_hash_packets(&packets);

    let merkle_tree = solana_merkle_tree::MerkleTree::new(&packets);

    let path_proofs: Vec<_> = (0..packets.len())
        .map(|i| merkle_tree.find_path(i).unwrap())
        .collect();

    let leaf_hashes: Vec<_> = packets.iter().map(|p| hashv(&[LEAF_PREFIX, p])).collect();

    b.iter(|| {
        for i in 0..packets.len() {
            //let leaf_hash = hashv(&[LEAF_PREFIX, hashes[i].get_bytes()]);
            //let leaf_hash = hashv(&[LEAF_PREFIX, &packets[i]]);
            //assert!(path_proofs[i].verify(leaf_hash));
            assert!(path_proofs[i].verify(leaf_hashes[i]));
        }
    });
}

#[bench]
fn bench_merk_sha(b: &mut Bencher) {
    let packets: Vec<_> = (0..FEC_SET_SIZE)
        .map(|_| test_create_random_packet_data())
        .collect();

    let hashes: Vec<_> = packets.iter().map(|p| Sha256::digest(p)).collect();

    b.iter(|| {
        let _ = hashv(&[&hashes[0], &hashes[1]]);
    });
}
