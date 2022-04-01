use {
    blstrs::Scalar,
    kzg::{
        coeff_form::KZGProver, coeff_form::KZGVerifier, polynomial::Polynomial, setup, KZGParams,
    },
    sha2::{Digest, Sha256},
    solana_merkle_tree::MerkleTree,
    std::time::Instant,
};

pub fn test_create_random_packet_data() -> Vec<u8> {
    let random_bytes: Vec<u8> = (0..1200).map(|_| rand::random::<u8>()).collect();
    random_bytes
}

pub fn test_create_trusted_setup(max_coeffs: usize) -> KZGParams {
    let s: Scalar = rand::random::<u64>().into();
    setup(s, max_coeffs)
}

pub fn hash_packets(packets: &Vec<Vec<u8>>) -> Vec<[u8; 32]> {
    packets
        .iter()
        .map(|p| {
            let mut h = <[u8; 32]>::try_from(Sha256::digest(p).as_slice()).unwrap();
            // truncate hash to 254 bits for conversion to Scalar
            h[31] &= 0b00111111u8;
            h
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_program::hash::{hashv, Hash};

    #[test]
    fn test_merkle() {
        const FEC_SET_SIZE: usize = 96;

        let packets: Vec<_> = (0..FEC_SET_SIZE)
            .map(|_| test_create_random_packet_data())
            .collect();

        let hashes: Vec<[u8; 32]> = hash_packets(&packets);

        let now = Instant::now();
        let merkle_tree = MerkleTree::new(&hashes);
        println!("create_merkle_tree_us: {}", now.elapsed().as_micros());

        let root_hash = merkle_tree.get_root();

        //let path_proof = merkle_tree.find_path(33).unwrap();
        //println!("path_proof: {:?}", &path_proof);

        const LEAF_PREFIX: &[u8] = &[0];
        const INTERMEDIATE_PREFIX: &[u8] = &[1];

        //let hash = hash_leaf!(hashes[33]);

        let now = Instant::now();
        let path_proofs: Vec<_> = (0..packets.len())
            .map(|i| merkle_tree.find_path(i).unwrap())
            .collect();
        println!("creat_proofs_us: {}", now.elapsed().as_micros());

        let now = Instant::now();
        for i in 0..packets.len() {
            let leaf_hash = hashv(&[LEAF_PREFIX, &hashes[i]]);
            assert!(path_proofs[i].verify(leaf_hash));
        }
        println!("merkle_verify_us: {}", now.elapsed().as_micros());

        //let hash = hashv(&[LEAF_PREFIX, &hashes[33]]);
        //assert!(path_proof.verify(hash));
    }

    fn recompute(mut start: [u8; 32], path: &[[u8; 32]], address: u32) -> [u8; 32] {
        for (ix, s) in path.iter().enumerate() {
            if address >> ix & 1 == 1 {
                let res = hashv(&[&start, s.as_ref()]);
                start.copy_from_slice(res.as_ref());
            } else {
                let res = hashv(&[s.as_ref(), &start]);
                start.copy_from_slice(res.as_ref());
            }
        }
        start
    }

    #[test]
    fn test_kzg_batch() {
        const FEC_SET_SIZE: usize = 96;
        let params = test_create_trusted_setup(FEC_SET_SIZE);
        let prover = KZGProver::new(&params);
        let verifier = KZGVerifier::new(&params);

        let packets: Vec<_> = (0..FEC_SET_SIZE)
            .map(|_| test_create_random_packet_data())
            .collect();

        let now = Instant::now();
        let hashes: Vec<[u8; 32]> = hash_packets(&packets);
        let elapsed = now.elapsed();
        println!(
            "sha256_us batch={} packet={}",
            elapsed.as_micros(),
            (elapsed / FEC_SET_SIZE as u32).as_micros()
        );

        let mut xs = Vec::default();
        for i in 0..FEC_SET_SIZE {
            xs.push(Scalar::from_u64s_le(&[0, 0, 0, i as u64]).unwrap());
        }
        let ys: Vec<_> = hashes
            .iter()
            .map(|h| Scalar::from_bytes_le(&h).unwrap())
            .collect();

        let now = Instant::now();
        let interpolation = Polynomial::lagrange_interpolation(&xs, &ys);
        println!("lagrange_interpolation_us: {}", now.elapsed().as_micros());

        let now = Instant::now();
        let commitment = prover.commit(&interpolation);
        println!("commit_time_us: {}", now.elapsed().as_micros());

        let now = Instant::now();
        assert!(verifier.verify_poly(&commitment, &interpolation));
        println!("verify_commitment_us: {}", now.elapsed().as_micros());

        let now = Instant::now();
        let witnesses: Vec<_> = xs
            .iter()
            .zip(ys.iter())
            .map(|(x, y)| prover.create_witness(&interpolation, (*x, *y)).unwrap())
            .collect();
        let elapsed = now.elapsed();
        println!(
            "witnesses_time_us: batch={} packet={}",
            elapsed.as_micros(),
            (elapsed / FEC_SET_SIZE as u32).as_micros()
        );

        let now = Instant::now();
        for i in 0..FEC_SET_SIZE {
            assert!(verifier.verify_eval((xs[i], ys[i]), &commitment, &witnesses[i]));
        }
        let elapsed = now.elapsed();
        println!(
            "verify_time_us: batch={} packet={}",
            elapsed.as_micros(),
            (elapsed / FEC_SET_SIZE as u32).as_micros()
        );
    }
}
